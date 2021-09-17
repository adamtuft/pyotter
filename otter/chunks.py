import otf2
from typing import Iterable
from collections import deque, defaultdict
from otter.definitions import EventType, RegionType, TaskStatus, TaskType, Endpoint, EdgeType
from otter.trace import AttributeLookup, event_defines_new_chunk
from otf2.events import Enter, Leave, ThreadTaskCreate, ThreadTaskSwitch, ThreadTaskComplete, ThreadBegin, ThreadEnd

class Chunk:
    """A sequence of events delineated by events at which the execution flow may diverge. Contains a reference to an
    AttributeLookup for looking up event attributes by name."""

    def __init__(self, events: Iterable, attr: AttributeLookup):
        self.attr = attr
        self.events = deque(events)

    def __repr__(self):
        s = "  {:18s} {:10s} {:20s} {:20s} {:18s} {}\n".format("Time", "Endpoint", "Region Type", "Event Type", "Region ID/Name", "Encountering Task")
        for e in self.events:
            try:
                s += "  {:<18d} {:10s} {:20s} {:20s} {:18s} {}\n".format(
                    e.time,
                    e.attributes[self.attr['endpoint']],
                    e.attributes.get(self.attr['region_type'], ""),
                    e.attributes[self.attr['event_type']],
                    str(e.attributes[self.attr['unique_id']]) if self.attr['unique_id'] in e.attributes else e.region.name,
                    e.attributes[self.attr['encountering_task_id']])
            except KeyError as err:
                print(err)
                raise
        return s

    def append(self, item):
        if type(item) is Chunk:
            raise TypeError()
        self.events.append(item)

    @property
    def first(self):
        return None if len(self.events) == 0 else self.events[0]

    @property
    def last(self):
        return None if len(self.events) == 0 else self.events[-1]

    def __len__(self):
        return len(self.events)

    @property
    def len(self):
        return len(self.events)

    @property
    def kind(self):
        return None if len(self.events) == 0 else event.attributes[self.events[0].attributes[self.attr['region_type']]]


def event_defines_new_task_fragment(e: otf2.events._EventMeta, a: AttributeLookup) -> bool:
    return (
        (type(e) in [ThreadTaskSwitch]) or
        (type(e) in [Enter, Leave] and e.attributes.get(a['region_type'], None) in
            [RegionType.parallel, RegionType.initial_task, RegionType.implicit_task, RegionType.single_executor, RegionType.master])
    )
    # return type(e) in [Enter, Leave, ThreadTaskSwitch]
    # return (e.attributes.get(a['region_type'], None) in ['parallel',
    #     'explicit_task', 'initial_task', 'single_executor', 'master'])


class ChunkGenerator:
    """Yields a sequence of Chunks by consuming the sequence of events in a trace."""

    def __init__(self, trace, key: str = None):
        self.events = trace.events
        self.attr = AttributeLookup(trace.definitions.attributes)
        self.strings = trace.definitions.strings
        self.key = key or "encountering_task_id"
        self._chunk_dict = defaultdict(lambda : Chunk(list(), self.attr))
        self._task_chunk_map = defaultdict(lambda : Chunk(list(), self.attr))

    def __getitem__(self, key):
        return self._task_chunk_map[key]

    def __setitem__(self, key, value):
        if type(value) is not Chunk:
            raise TypeError()
        self._task_chunk_map[key] = value

    def keys(self):
        return self._task_chunk_map.keys()

    def __iter__(self):

        # Map thread ID -> ID of current parallel region at top of deque
        current_parallel_region = defaultdict(deque)

        # Record the enclosing chunk when an event indicates a nested chunk
        chunk_stack = defaultdict(deque)

        # Not sure this is required anymore
        enclosing_task = dict()

        print("yielding chunks:", end="\n", flush=True)

        # Count chunks yielded
        nChunks = 0

        # Encountering task interpretation:
        # enter/leave:        the task that encountered the region associated with the event
        # task-switch:        the task that encountered the task-switch event i.e. the task being suspended
        # task-create:        the parent task of the created task

        for location, event in self.events:

            # Ignore thread-begin/end entirely
            if type(event) in [ThreadBegin, ThreadEnd]:
                continue

            # Used to lookup the chunk currently being generated by the 
            # encountering task
            try:
                encountering_task = event.attributes[self.attr['encountering_task_id']]
            except KeyError:
                print(event)
                print(event.attributes)
                raise
            region_type = event.attributes.get(self.attr['region_type'], None)
            unique_id = event.attributes.get(self.attr['unique_id'], None)
            chunk_key = encountering_task

            if event_defines_new_task_fragment(event, self.attr):

                if isinstance(event, Enter):

                    # For initial-task-begin, chunk key is (thread ID, initial task unique_id)
                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id)
                        # append event

                    # For parallel-begin, chunk_key is (thread ID, parallel ID)
                    # parallel-begin is reported by master AND worker threads
                    # master thread treats it as a chunk boundary (i.e. it nests within the encountering chunk)
                    # worker thread treats it as a new chunk (i.e. it is not a nested chunk)
                    # Record parallel ID as the current parallel region for this thread
                    elif region_type == RegionType.parallel:
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                            # append event to current chunk for key (thread ID, encountering_task)
                            # assign a reference to this chunk to key (thread ID, parallel ID)
                            # push reference to this chunk onto chunk_stack at key (thread ID, parallel ID)
                            # append event to NEW chunk for key (thread ID, parallel ID)
                        # else:
                            # append event to NEW chunk with key (thread ID, parallel ID)

                    # For implicit-task-enter, chunk_key is encountering_task_id but this should map to the same chunk as (thread ID, parallel ID)
                    # so that later events in this task are recorded against the same chunk
                    elif region_type == RegionType.implicit_task:
                        chunk_key = (location.name, {this threads current parallel region})
                        self[chunk_key].append(event)
                        # Ensure implicit-task-id points to this chunk for later events in this task
                        self[unique_id] = self[chunk_key]

                    elif region_type in [RegionType.single_executor, RegionType.master]
                        # do the stack-push thing

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                elif isinstance(event, Leave):

                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id)
                        # append event
                        # yield chunk

                    elif region_type == RegionType.parallel:
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                            # append event to chunk for key (thread ID, parallel ID)
                            # yield this chunk
                            # pop from chunk_stack at key (thread ID, parallel ID) and overwrite at this key in self[(thread ID, parallel ID)]
                            # append event to now-popped chunk for key (thread ID, parallel ID) which is the one containing the enclosing initial task events
                            # update self[(thread ID, encountering task ID)] to refer to the same chunk as self[(thread ID, parallel ID)]
                        # else:
                            # append event
                            # yield chunk

                    elif region_type == RegionType.implicit_task:
                        self[unique_id].append(event)
                        # continue - don't yield until parallel-end

                    elif region_type in [RegionType.single_executor, RegionType.master]
                        # do the stack-pop thing
                        # yield chunk

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                    # nChunks += 1
                    # if nChunks % 100 == 0:
                    #     if nChunks % 1000 == 0:
                    #         print("yielding chunks:", end=" ", flush=True)
                    #     print(f"{nChunks:4d}", end="", flush=True)
                    # elif nChunks % 20 == 0:
                    #     print(".", end="", flush=True)
                    # if (nChunks+1) % 1000 == 0:
                    #     print("", flush=True)
                    # print(f">> Yielding chunk:")
                    # print(self[encountering_task])
                    # yield self[encountering_task]
                    # if region_type in [RegionType.initial_task]:
                    #     task_left = event.attributes[self.attr['unique_id']]
                    #     self[task_left].append(event)
                    #     print(f">> Yielding chunk:")
                    #     print(self[task_left])
                    #     yield self[task_left]
                    #     # self[encountering_task] = chunk_stack[task_left].pop()
                    # elif region_type in [RegionType.implicit_task]:
                    #     task_left = event.attributes[self.attr['unique_id']]
                    #     self[task_left].append(event)
                    #     self[encountering_task].append(event)
                    # else:
                    #     # Continue with enclosing chunk, if there is one
                    #     self[encountering_task].append(event)
                    #     print(f">> Yielding chunk:")
                    #     print(self[encountering_task])
                    #     yield self[encountering_task]
                    #     try:
                    #         self[encountering_task] = chunk_stack[encountering_task].pop()
                    #         self[encountering_task].append(event)
                    #     except IndexError as err:
                    #         print(f"Error: {str(err)}")
                    #         print("Error when processing this event:")
                    #         print(fmt_event(event, self.attr))

                elif isinstance(event, ThreadTaskSwitch):
                    prior_task_status = event.attributes[self.attr['prior_task_status']]
                    prior_task_id = event.attributes[self.attr['prior_task_id']]
                    next_task_id = event.attributes[self.attr['next_task_id']]
                    self[encountering_task].append(event)
                    if prior_task_status in [TaskStatus.complete]:
                        print(f">> Yielding chunk (task {prior_task_id} complete):")
                        print(self[encountering_task])
                        yield self[encountering_task]
                    self[next_task_id].append(event)

                else:
                    self[encountering_task].append(event)

            else:
                self[encountering_task].append(event)
        print("")

def fmt_event(e, attr):
    s = ""
    try:
        s += "  {:<18d} {:10s} {:20s} {:20s} {:18s} {}".format(
            e.time,
            e.attributes[attr['endpoint']],
            e.attributes.get(attr['region_type'], ""),
            e.attributes[attr['event_type']],
            str(e.attributes[attr['unique_id']]) if attr['unique_id'] in e.attributes else e.region.name,
            e.attributes[attr['encountering_task_id']])
    except KeyError as err:
        print(err)
        print(type(e))
    return s