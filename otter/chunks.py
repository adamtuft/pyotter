import otf2
import igraph as ig
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
        s = "Chunk with {} events:\n".format(len(self.events))
        s += "  {:18s} {:10s} {:20s} {:20s} {:18s} {}\n".format("Time", "Endpoint", "Region Type", "Event Type", "Region ID/Name", "Encountering Task")
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

    def items(self):
        return (None, self.events)


class ChunkGenerator:
    """Yields a sequence of Chunks by consuming the sequence of events in a trace."""

    def __init__(self, trace, verbose: bool = False):
        self.events = trace.events
        self.attr = AttributeLookup(trace.definitions.attributes)
        self.strings = trace.definitions.strings
        self._chunk_dict = defaultdict(lambda : Chunk(list(), self.attr))
        self._task_chunk_map = defaultdict(lambda : Chunk(list(), self.attr))
        self.verbose = verbose
        self.nChunks = 0
        self.events_consumed = 0
        self.task_links = deque()
        self.task_type = dict()
        self.task_crt_ts = dict()
        self.task_end_ts = dict()
        self.task_switch_events = defaultdict(deque)

    def __getitem__(self, key):
        return self._task_chunk_map[key]

    def __setitem__(self, key, value):
        if type(value) is not Chunk:
            raise TypeError()
        self._task_chunk_map[key] = value

    def keys(self):
        return self._task_chunk_map.keys()

    def make_task_tree(self):
        start_time = self.task_crt_ts[0]
        last_time = max(self.task_end_ts.values())
        duration = last_time - start_time
        task_ids = list(self.task_type.keys())
        task_tree = ig.Graph(n=len(task_ids), directed=True)
        task_tree.vs['name'] = task_ids
        task_tree.vs['task_type'] = [self.task_type[k] for k in task_ids]
        task_tree.vs['crt_ts'] = [self.task_crt_ts[k] for k in task_ids]
        task_tree.vs['end_ts'] = [self.task_end_ts[k] for k in task_ids]
        for parent, child in self.task_links:
            if child == 0:
                continue
            task_tree.add_edge(parent, child)
        return task_tree

    def yield_chunk(self, chunk_key):
        if self.verbose:
            self.nChunks += 1
            if self.nChunks % 100 == 0:
                if self.nChunks % 1000 == 0:
                    print("yielding chunks:", end=" ", flush=True)
                print(f"{self.nChunks:4d}", end="", flush=True)
            elif self.nChunks % 20 == 0:
                print(".", end="", flush=True)
            if (self.nChunks+1) % 1000 == 0:
                print("", flush=True)
        yield self[chunk_key]

    def __iter__(self):

        # Map thread ID -> ID of current parallel region at top of deque
        current_parallel_region = defaultdict(deque)

        # Record the enclosing chunk when an event indicates a nested chunk
        chunk_stack = defaultdict(deque)

        if self.verbose:
            print("yielding chunks:", end="\n", flush=True)

        # Encountering task interpretation:
        # enter/leave:        the task that encountered the region associated with the event
        # task-switch:        the task that encountered the task-switch event i.e. the task being suspended
        # task-create:        the parent task of the created task

        for location, event in self.events:
            self.events_consumed += 1

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

            # default:
            chunk_key = encountering_task

            # Record task links, creation time and task-switch events for later extraction of timing data
            if (type(event) == ThreadTaskCreate) or (type(event) == Enter and event.attributes[self.attr['region_type']] in [RegionType.initial_task, RegionType.implicit_task]):
                self.task_links.append((encountering_task, unique_id))
                self.task_type[unique_id] = event.attributes[self.attr['region_type']]
                self.task_crt_ts[unique_id] = event.time
            elif (type(event) == ThreadTaskSwitch):
                print(f">> Switch {encountering_task} -> {unique_id}")
                self.task_switch_events[encountering_task].append(event)
                self.task_switch_events[unique_id].append(event)
                prior_task_status = event.attributes[self.attr['prior_task_status']]
                if prior_task_status == TaskStatus.complete:
                    self.task_end_ts[encountering_task] = event.time
            elif type(event) == Leave and event.attributes[self.attr['region_type']] in [RegionType.initial_task, RegionType.implicit_task]:
                self.task_end_ts[unique_id] = event.time

            if event_defines_new_task_fragment(event, self.attr):

                if isinstance(event, Enter):

                    # For initial-task-begin, chunk key is (thread ID, initial task unique_id)
                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id, 't')
                        self[chunk_key].append(event)

                    # For parallel-begin, chunk_key is (thread ID, parallel ID)
                    # parallel-begin is reported by master AND worker threads
                    # master thread treats it as a chunk boundary (i.e. it nests within the encountering chunk)
                    # worker thread treats it as a new chunk (i.e. it is not a nested chunk)
                    # Record parallel ID as the current parallel region for this thread
                    elif region_type == RegionType.parallel:
                        encountering_task_key = (location.name, encountering_task, 't')
                        parallel_region_key = (location.name, unique_id, 'p')
                        current_parallel_region[location.name].append(unique_id)
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                        if encountering_task_key in self.keys():
                            # append event to current chunk for key (thread ID, encountering_task)
                            self[encountering_task_key].append(event)
                            # assign a reference to this chunk to key (thread ID, parallel ID)
                            self[parallel_region_key] = self[encountering_task_key]
                            # push reference to this chunk onto chunk_stack at key (thread ID, parallel ID)
                            chunk_stack[parallel_region_key].append(self[parallel_region_key])
                            # append event to NEW chunk for key (thread ID, parallel ID)
                            self[parallel_region_key] = Chunk((event,), self.attr)

                        else:
                            # append event to NEW chunk with key (thread ID, parallel ID)
                            self[parallel_region_key].append(event)

                    # For implicit-task-enter, chunk_key is encountering_task_id but this must be made to refer to the same chunk as (thread ID, parallel ID)
                    # so that later events in this task are recorded against the same chunk
                    elif region_type == RegionType.implicit_task:
                        parallel_id = current_parallel_region[location.name][-1]
                        chunk_key = (location.name, parallel_id, 'p')
                        self[chunk_key].append(event)
                        # Ensure implicit-task-id points to this chunk for later events in this task
                        self[unique_id] = self[chunk_key]

                    elif region_type in [RegionType.single_executor, RegionType.master]:
                        # do the stack-push thing
                        self[chunk_key].append(event)
                        chunk_stack[chunk_key].append(self[chunk_key])
                        self[chunk_key] = Chunk((event,), self.attr)

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                elif isinstance(event, Leave):

                    if region_type == RegionType.initial_task:
                        chunk_key = (location.name, unique_id, 't')
                        # append event
                        self[chunk_key].append(event)
                        # yield chunk
                        yield from self.yield_chunk(chunk_key)

                    elif region_type == RegionType.parallel:
                        encountering_task_key = (location.name, encountering_task, 't')
                        parallel_region_key = (location.name, unique_id, 'p')
                        current_parallel_region[location.name].pop()
                        # if master thread: (does the key (thread ID, encountering_task) exist in self.keys())
                        if encountering_task_key in self.keys():
                            # append event to chunk for key (thread ID, parallel ID)
                            self[parallel_region_key].append(event)
                            # yield this chunk
                            yield from self.yield_chunk(parallel_region_key)
                            # pop from chunk_stack at key (thread ID, parallel ID) and overwrite at this key in self[(thread ID, parallel ID)]
                            self[parallel_region_key] = chunk_stack[parallel_region_key].pop()
                            # append event to now-popped chunk for key (thread ID, parallel ID) which is the one containing the enclosing initial task events
                            self[parallel_region_key].append(event)
                            # update self[(thread ID, encountering task ID)] to refer to the same chunk as self[(thread ID, parallel ID)]
                            self[encountering_task_key] = self[parallel_region_key]
                        else:
                            # append event
                            self[parallel_region_key].append(event)
                            # yield chunk
                            yield from self.yield_chunk(parallel_region_key)

                    elif region_type == RegionType.implicit_task:
                        self[unique_id].append(event)
                        # continue - don't yield until parallel-end

                    elif region_type in [RegionType.single_executor, RegionType.master]:
                        # do the stack-pop thing
                        self[chunk_key].append(event)
                        # yield chunk
                        yield from self.yield_chunk(chunk_key)
                        self[chunk_key] = chunk_stack[chunk_key].pop()
                        self[chunk_key].append(event)

                    else:
                        # Nothing should get here
                        print(event)
                        raise ValueError("shouldn't be here")

                elif isinstance(event, ThreadTaskSwitch):
                    # encountering task id == prior task
                    prior_task_status = event.attributes[self.attr['prior_task_status']]
                    prior_task_id = event.attributes[self.attr['prior_task_id']]
                    next_task_id = event.attributes[self.attr['next_task_id']]
                    self[chunk_key].append(event)
                    if prior_task_status in [TaskStatus.complete]:
                        yield from self.yield_chunk(chunk_key)
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


def event_defines_new_task_fragment(e: otf2.events._EventMeta, a: AttributeLookup) -> bool:
    return (
        (type(e) in [ThreadTaskSwitch]) or
        (type(e) in [Enter, Leave] and e.attributes.get(a['region_type'], None) in
            [RegionType.parallel, RegionType.initial_task, RegionType.implicit_task, RegionType.single_executor, RegionType.master])
    )