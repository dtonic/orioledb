/*-------------------------------------------------------------------------
 *
 * queue.c
 *		Implementation for queue of tasks for S3 workers.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "s3/queue.h"

#include "utils/wait_event.h"

/*
 * Meta-information about S3 tasks queue.
 */
typedef struct
{
	/* Location to insert new tasks */
	pg_atomic_uint64 insertLocation;
	ConditionVariable insertLocationCV;

	/* Location to pick the existing tasks by workers */
	pg_atomic_uint64 pickLocation;

	/*
	 * All the tasks before this location has been already erased in the
	 * buffer.
	 */
	pg_atomic_uint64 erasedLocation;
	ConditionVariable erasedLocationCV;

	/* the background lane's tasks the workers are running (lane 0 only) */
	pg_atomic_uint32 backgroundRunning;
} S3TaskQueueMeta;

/*
 * The "erased" flag for the task length.  This flag means that task body was
 * already erased, but erased position wasn't yet advanced through this task.
 */
#define	LENGTH_ERASED_FLAG	(0x80000000)

/*
 * Two lanes of tasks, each its own circular buffer: the tasks everything else
 * waits for, and the background lane (deletes of WAL chunks a GC frees).  A
 * worker takes a background task only when the first lane has none, and no
 * more than all the workers but one run background tasks at a time: after a
 * start, a GC's deletes in one queue kept every worker for a minute and the
 * reads, uploads and maps behind them waited.  Nothing waits for a background
 * task's location.  Its locations carry BACKGROUND_LOCATION_BIT.
 */
typedef struct
{
	S3TaskQueueMeta *meta;
	Pointer		buffer;
	Size		size;
} S3TaskQueueLane;

#define BACKGROUND_LOCATION_BIT	(UINT64CONST(1) << 62)

static S3TaskQueueLane lanes[2];

#define FOREGROUND_LANE	(&lanes[0])
#define BACKGROUND_LANE	(&lanes[1])

/*
 * The length word of the task at a location.  Lengths are INTALIGN'd and the
 * buffer's size is a multiple of 1 kB, so the word never straddles the end.
 * It is read and written atomically: a producer publishes a task with it,
 * and the eraser that advances the erased location past a task claims the
 * task by zeroing it (s3_queue_erase_task).
 */
static inline pg_atomic_uint32 *
lane_length_word(S3TaskQueueLane *lane, S3TaskLocation location)
{
	return (pg_atomic_uint32 *) (lane->buffer + location % lane->size);
}

static Size
background_lane_size(void)
{
	return Max((Size) s3_queue_size_guc * 1024 / 4, (Size) 64 * 1024);
}

Size
s3_queue_shmem_needs(void)
{
	Size		size = 0;

	if (!orioledb_s3_mode)
		return size;

	size = add_size(size, CACHELINEALIGN(sizeof(S3TaskQueueMeta)));
	size = add_size(size, CACHELINEALIGN((Size) s3_queue_size_guc * 1024));
	size = add_size(size, CACHELINEALIGN(sizeof(S3TaskQueueMeta)));
	size = add_size(size, CACHELINEALIGN(background_lane_size()));

	return size;
}

static Pointer
lane_init_shmem(S3TaskQueueLane *lane, Pointer ptr, Size size, bool found)
{
	lane->size = size;
	lane->meta = (S3TaskQueueMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3TaskQueueMeta));
	lane->buffer = ptr;
	ptr += CACHELINEALIGN(size);

	if (!found)
	{
		/*
		 * Start one buffer length in, at the same offset: no task is at
		 * location 0, which callers read as "nothing to wait for"
		 * (s3_load_map_file()).  The first task of a start was at 0, and a
		 * map load that was the first returned before the map was there: the
		 * tree read as empty, and a load lost its rows.
		 */
		pg_atomic_init_u64(&lane->meta->insertLocation, size);
		pg_atomic_init_u64(&lane->meta->pickLocation, size);
		pg_atomic_init_u64(&lane->meta->erasedLocation, size);
		pg_atomic_init_u32(&lane->meta->backgroundRunning, 0);

		ConditionVariableInit(&lane->meta->insertLocationCV);
		ConditionVariableInit(&lane->meta->erasedLocationCV);

		memset(lane->buffer, 0, size);
	}
	return ptr;
}

void
s3_queue_init_shmem(Pointer ptr, bool found)
{
	if (!orioledb_s3_mode)
		return;

	ptr = lane_init_shmem(FOREGROUND_LANE, ptr, (Size) s3_queue_size_guc * 1024,
						  found);
	(void) lane_init_shmem(BACKGROUND_LANE, ptr, background_lane_size(), found);
}

S3TaskLocation
s3_queue_get_insert_location(void)
{
	return pg_atomic_read_u64(&FOREGROUND_LANE->meta->insertLocation);
}

static S3TaskQueueLane *
lane_of(S3TaskLocation *location)
{
	if (*location & BACKGROUND_LOCATION_BIT)
	{
		*location &= ~BACKGROUND_LOCATION_BIT;
		return BACKGROUND_LANE;
	}
	return FOREGROUND_LANE;
}

static S3TaskLocation
lane_put_task(S3TaskQueueLane *lane, Pointer data, uint32 len)
{
	S3TaskLocation insertLocation;
	bool		slept = false;
	uint32		totallen = len + sizeof(uint32);
	S3TaskQueueMeta *meta = lane->meta;
	Pointer		buffer = lane->buffer;
	Size		size = lane->size;

	Assert(totallen == INTALIGN(totallen));

	/* Pick the insert location */
	insertLocation = pg_atomic_fetch_add_u64(&meta->insertLocation, totallen);

	/*
	 * Check that circular buffer of tasks didn't wraparound.  Wait the
	 * overlapping tasks to be erased before we continue.
	 */
	while (insertLocation + totallen > pg_atomic_read_u64(&meta->erasedLocation) + size)
	{
		(void) ConditionVariableTimedSleep(&meta->erasedLocationCV,
										   1000, WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();

	/* Put the task into a circular buffer */
	if (insertLocation / size == (insertLocation + totallen - 1) / size)
	{
		/* Easy case: we can put the task a as continuous chunk of memory */
		memcpy(buffer + insertLocation % size + sizeof(uint32),
			   data,
			   len);
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * need to split the task into two distinct chunks.
		 */
		uint32		firstChunkLen = size - insertLocation % size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(buffer + insertLocation % size + sizeof(uint32),
			   data,
			   firstChunkLen - sizeof(uint32));
		memcpy(buffer,
			   data + (firstChunkLen - sizeof(uint32)),
			   totallen - firstChunkLen);
	}

	/*
	 * Write the task length after copying the task body.  We use length
	 * presence as the sign that body is completely copied.
	 */
	pg_write_barrier();
	pg_atomic_write_u32(lane_length_word(lane, insertLocation), totallen);

	/*
	 * Wake a worker sleeping in s3_queue_wait_for_task().  Without it the
	 * task waited for a worker's next look at the queue, bgwriter_delay
	 * apart: a backend reading evicted parts one by one got about 20 MB/s
	 * from a store that serves a part in 5 ms.
	 */
	ConditionVariableSignal(&FOREGROUND_LANE->meta->insertLocationCV);

	return insertLocation;
}

/*
 * Put new task to the lockless queue.
 */
S3TaskLocation
s3_queue_put_task(Pointer data, uint32 len)
{
	return lane_put_task(FOREGROUND_LANE, data, len);
}

/*
 * Put a task to the background lane: nobody waits for it.
 */
void
s3_queue_put_background_task(Pointer data, uint32 len)
{
	(void) lane_put_task(BACKGROUND_LANE, data, len);
}

/*
 * Whether a task is there to pick: the conditions lane_try_pick_task() picks
 * on, without picking.
 */
static bool
lane_task_ready(S3TaskQueueLane *lane)
{
	S3TaskLocation insertLocation,
				pickLocation,
				erasedLocation;
	uint32		taskLen;

	pickLocation = pg_atomic_read_u64(&lane->meta->pickLocation);
	pg_read_barrier();
	insertLocation = pg_atomic_read_u64(&lane->meta->insertLocation);
	erasedLocation = pg_atomic_read_u64(&lane->meta->erasedLocation);

	if (pickLocation >= insertLocation ||
		pickLocation + sizeof(uint32) >= erasedLocation + lane->size)
		return false;

	taskLen = pg_atomic_read_u32(lane_length_word(lane, pickLocation));
	return taskLen != 0 && !(taskLen & LENGTH_ERASED_FLAG);
}

/* all the workers but one may run background tasks at a time */
static bool
background_slot_free(void)
{
	return pg_atomic_read_u32(&FOREGROUND_LANE->meta->backgroundRunning) <
		(uint32) Max(s3_num_workers - 1, 1);
}

/*
 * Sleep until a task is put or the timeout passes.  The worker prepares to
 * sleep before it looks at the queue, so a task put after the look wakes it
 * (lane_put_task() signals after it wrote the task's length).  A background
 * task waiting for a slot does not wake it: the worker that frees the slot
 * runs on to the next one.
 */
void
s3_queue_wait_for_task(long timeout, uint32 wait_event_info)
{
	ConditionVariablePrepareToSleep(&FOREGROUND_LANE->meta->insertLocationCV);
	if (!lane_task_ready(FOREGROUND_LANE) &&
		!(lane_task_ready(BACKGROUND_LANE) && background_slot_free()))
		(void) ConditionVariableTimedSleep(&FOREGROUND_LANE->meta->insertLocationCV,
										   timeout, wait_event_info);
	ConditionVariableCancelSleep();
}

/*
 * Pick the lane's next task.  Its location, with laneBits, is written to
 * *record before the pick takes it: a worker that dies right after the pick
 * then replays the task (s3worker_main), rather than leaving it picked by
 * nobody with the erased location stuck behind it.  A location recorded for
 * a pick that did not take it is cleared again.
 */
static S3TaskLocation
lane_try_pick_task(S3TaskQueueLane *lane, S3TaskLocation laneBits,
				   volatile S3TaskLocation *record)
{
	while (true)
	{
		S3TaskLocation insertLocation,
					pickLocation,
					erasedLocation;
		uint32		taskLen;

		pickLocation = pg_atomic_read_u64(&lane->meta->pickLocation);
		pg_read_barrier();
		insertLocation = pg_atomic_read_u64(&lane->meta->insertLocation);
		erasedLocation = pg_atomic_read_u64(&lane->meta->erasedLocation);

		if (pickLocation >= insertLocation)
		{
			/* Nothing inserted yet */
			Assert(pickLocation == insertLocation);
			return InvalidS3TaskLocation;
		}

		if (pickLocation + sizeof(uint32) >= erasedLocation + lane->size)
		{
			/* Insert location is advanced, but the area wasn't erased yet */
			return InvalidS3TaskLocation;
		}

		taskLen = pg_atomic_read_u32(lane_length_word(lane, pickLocation));

		/*
		 * Another worker picked, did and erased the task since the pick
		 * location was read: read it again.  Woken workers and read-ahead
		 * hints, which are done at once when the part is already here, made
		 * this common.
		 */
		if (taskLen & LENGTH_ERASED_FLAG)
			continue;

		if (taskLen == 0)
		{
			/* Insert location is advanced, but the data wasn't written yet */
			return InvalidS3TaskLocation;
		}

		/*
		 * Try to advance the pick location.  Whoever succeed on advancing the
		 * pick location is assumed to successfully pick the task.
		 */
		if (record)
			*record = pickLocation | laneBits;
		if (pg_atomic_compare_exchange_u64(&lane->meta->pickLocation,
										   &pickLocation,
										   pickLocation + taskLen))
		{
			return pickLocation;
		}
		if (record)
			*record = InvalidS3TaskLocation;
	}
}

/*
 * Try to pick the task for processing.  Returns the task location on success,
 * and InvalidS3TaskLocation on failure.  The first lane goes first; a
 * background task is taken in a free slot.
 */
S3TaskLocation
s3_queue_try_pick_task(volatile S3TaskLocation *record)
{
	S3TaskLocation location;
	pg_atomic_uint32 *running = &FOREGROUND_LANE->meta->backgroundRunning;
	uint32		n;

	location = lane_try_pick_task(FOREGROUND_LANE, 0, record);
	if (location != InvalidS3TaskLocation)
		return location;

	/* take a slot, then a task; give the slot back if there is none */
	n = pg_atomic_read_u32(running);
	while (n < (uint32) Max(s3_num_workers - 1, 1))
	{
		if (pg_atomic_compare_exchange_u32(running, &n, n + 1))
		{
			location = lane_try_pick_task(BACKGROUND_LANE,
										  BACKGROUND_LOCATION_BIT, record);
			if (location != InvalidS3TaskLocation)
				return location | BACKGROUND_LOCATION_BIT;
			pg_atomic_fetch_sub_u32(running, 1);
			break;
		}
	}
	return InvalidS3TaskLocation;
}

/*
 * Whether the task at a location is picked and not yet erased: what a worker
 * restarted on a recorded location replays.  A location the erased location
 * has passed, or whose task is erased, is done (by this worker before it
 * died, or by another).
 */
bool
s3_queue_task_is_live(S3TaskLocation taskLocation)
{
	S3TaskQueueLane *lane = lane_of(&taskLocation);
	uint32		taskLen;

	if (taskLocation < pg_atomic_read_u64(&lane->meta->erasedLocation) ||
		taskLocation >= pg_atomic_read_u64(&lane->meta->pickLocation))
		return false;
	taskLen = pg_atomic_read_u32(lane_length_word(lane, taskLocation));
	return taskLen != 0 && !(taskLen & LENGTH_ERASED_FLAG);
}

/*
 * Get the task by its location.
 */
Pointer
s3_queue_get_task(S3TaskLocation taskLocation)
{
	S3TaskQueueLane *lane = lane_of(&taskLocation);
	uint32		taskLen;
	Pointer		result;
	Pointer		buffer = lane->buffer;
	Size		size = lane->size;

	/* Get the task length */
	taskLen = pg_atomic_read_u32(lane_length_word(lane, taskLocation));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	result = (Pointer) palloc(taskLen - sizeof(uint32));

	/* Copy the task body */
	if (taskLocation / size == (taskLocation + taskLen - 1) / size)
	{
		/* Easy case: the task is a continuous chunk of memory */
		memcpy(result,
			   buffer + taskLocation % size + sizeof(uint32),
			   taskLen - sizeof(uint32));
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * have to assemble task from the two distinct chunks.
		 */
		uint32		firstChunkLen = size - taskLocation % size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(result,
			   buffer + taskLocation % size + sizeof(uint32),
			   firstChunkLen - sizeof(uint32));
		memcpy(result + (firstChunkLen - sizeof(uint32)),
			   buffer,
			   taskLen - firstChunkLen);
	}

	return result;
}

/*
 * Erase the processed task from the circular buffer.  A background task
 * gives its slot back.
 */
void
s3_queue_erase_task(S3TaskLocation taskLocation)
{
	S3TaskQueueLane *lane = lane_of(&taskLocation);
	S3TaskQueueMeta *meta = lane->meta;
	Pointer		buffer = lane->buffer;
	Size		size = lane->size;
	uint32		taskLen;

	taskLen = pg_atomic_read_u32(lane_length_word(lane, taskLocation));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	/* Erase the task body */
	if (taskLocation / size == (taskLocation + taskLen - 1) / size)
	{
		/* Easy case: the task is a continuous chunk of memory */
		memset(buffer + taskLocation % size + sizeof(uint32),
			   0,
			   taskLen - sizeof(uint32));
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * have to erase the two distinct chunks.
		 */
		int			firstChunkLen = size - taskLocation % size;

		Assert(firstChunkLen >= sizeof(uint32));

		memset(buffer + taskLocation % size + sizeof(uint32),
			   0,
			   firstChunkLen - sizeof(uint32));
		memset(buffer,
			   0,
			   taskLen - firstChunkLen);
	}

	pg_write_barrier();

	/* Put the LENGTH_ERASED_FLAG, which means we have erased the task body */
	pg_atomic_write_u32(lane_length_word(lane, taskLocation),
						taskLen | LENGTH_ERASED_FLAG);

	/*
	 * The flag must be visible before we read the erased location.  The
	 * eraser of the task before ours advances the location (a full barrier)
	 * and then reads our length word; we read the location after the flag.
	 * Without this barrier the load could pass the store (arm64): that eraser
	 * saw no flag and stopped, we saw the old location and gave up, and the
	 * lane stalled with erased tasks at its head (SNB SF1, S3 mode).
	 */
	pg_memory_barrier();

	/*
	 * Advance the erased location over this task, and over the erased tasks
	 * after it whose erasers gave up on advancing it (they found it behind
	 * their task; we take the lead), so somebody advances it past every
	 * erased task.
	 *
	 * The slot is zeroed before the erased location is published past it,
	 * never after: a producer blocked on the full ring writes its task into
	 * the slot as soon as the location passes, and a zero written after that
	 * wiped the new task's length (a length-0 task at the head stalled the
	 * lane).  Zeroing the flagged length word is also the claim: of two
	 * erasers at the head of the erased region, the one whose exchange
	 * zeroes it advances the location; the other leaves it to them.
	 */
	while (pg_atomic_read_u64(&meta->erasedLocation) == taskLocation)
	{
		uint32		flagged = taskLen | LENGTH_ERASED_FLAG;
		S3TaskLocation erased = taskLocation;

		if (!pg_atomic_compare_exchange_u32(lane_length_word(lane, taskLocation),
											&flagged, 0))
			break;				/* another eraser took this task */
		pg_write_barrier();
		if (!pg_atomic_compare_exchange_u64(&meta->erasedLocation, &erased,
											taskLocation + taskLen))
			elog(PANIC, "S3 task queue: erased location moved from " UINT64_FORMAT " while its task was claimed",
				 (uint64) taskLocation);

		taskLocation += taskLen;
		taskLen = pg_atomic_read_u32(lane_length_word(lane, taskLocation));
		if (!(taskLen & LENGTH_ERASED_FLAG))
			break;
		taskLen &= ~LENGTH_ERASED_FLAG;
	}

	ConditionVariableBroadcast(&meta->erasedLocationCV);

	if (lane == BACKGROUND_LANE)
		pg_atomic_fetch_sub_u32(&FOREGROUND_LANE->meta->backgroundRunning, 1);
}

/*
 * Wait till the task with given location is processed by worker.
 */
void
s3_queue_wait_for_location(S3TaskLocation location)
{
	S3TaskQueueMeta *meta = FOREGROUND_LANE->meta;
	bool		slept = false;

	Assert(!(location & BACKGROUND_LOCATION_BIT));

	/*
	 * Look again every second whatever the broadcasts say: a wakeup that is
	 * missed would otherwise leave the waiter asleep with the task long done
	 * (seen as a checkpoint that never finished, the queue empty).
	 */
	while (pg_atomic_read_u64(&meta->erasedLocation) <= location)
	{
		(void) ConditionVariableTimedSleep(&meta->erasedLocationCV,
										   1000, WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();
}
