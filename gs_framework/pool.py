import collections
import typing
from typing import Set, TypeVar, Generic, List
import heapq

# the pool interface is modified from
# https://commons.apache.org/proper/commons-pool/api-1.6/org/apache/commons/pool/ObjectPool.html

POOL_ITEM_TYPE = TypeVar('POOL_ITEM_TYPE')


class IdleObjectsHeap(Generic[POOL_ITEM_TYPE]):

    def __init__(self):
        super().__init__()
        self._heap: List[POOL_ITEM_TYPE] = []

    # return true means truly added, otherwise false
    def add_object(self, obj: POOL_ITEM_TYPE) -> bool:
        if obj not in self._heap:
            heapq.heappush(self._heap, obj)
            return True
        else:
            return False

    # return true means truly deleted, otherwise false
    def remove_object(self, obj: POOL_ITEM_TYPE) -> bool:
        try:
            self._heap.remove(obj)
            remove_from_idle_objects = True
        except ValueError:
            remove_from_idle_objects = False

        if remove_from_idle_objects:
            heapq.heapify(self._heap)

        return remove_from_idle_objects

    def borrow_object(self) -> POOL_ITEM_TYPE:
        try:
            obj = heapq.heappop(self._heap)
            print(f"{obj} has been borrowed from pool")
            return obj
        except IndexError:
            raise RuntimeError("No more idle object")

    def return_object(self, obj: POOL_ITEM_TYPE):
        assert obj not in self._heap
        heapq.heappush(self._heap, obj)
        print(f"{obj} has been returned to pool")

    def clear(self):
        self._heap.clear()

    def get_num(self) -> int:
        return len(self._heap)


class IdleObjectsFIFO(Generic[POOL_ITEM_TYPE]):

    def __init__(self):
        super().__init__()
        self._dict: typing.OrderedDict[POOL_ITEM_TYPE, int] = collections.OrderedDict()

    # return true means truly added, otherwise false
    def add_object(self, obj: POOL_ITEM_TYPE) -> bool:
        len_before_add = len(self._dict)
        self._dict[obj] = 1
        return len(self._dict) > len_before_add

    # return true means truly deleted, otherwise false
    def remove_object(self, obj: POOL_ITEM_TYPE) -> bool:
        return self._dict.pop(obj, 0) != 0

    def borrow_object(self) -> POOL_ITEM_TYPE:
        try:
            obj, _ = self._dict.popitem(last=False)
            print(f"{obj} has been borrowed from pool")
            return obj
        except KeyError:
            raise RuntimeError("No more idle object")

    def return_object(self, obj: POOL_ITEM_TYPE):
        assert obj not in self._dict
        self._dict[obj] = 1
        print(f"{obj} has been returned to pool")

    def clear(self):
        self._dict.clear()

    def get_num(self) -> int:
        return len(self._dict)


class Pool(Generic[POOL_ITEM_TYPE]):

    def __init__(self):
        super().__init__()
        self._active_objects: Set[POOL_ITEM_TYPE] = set()
        # self._idle_objects: IdleObjectsHeap[POOL_ITEM_TYPE] = IdleObjectsHeap()
        self._idle_objects: IdleObjectsFIFO[POOL_ITEM_TYPE] = IdleObjectsFIFO()

    # return true means truly added, otherwise false
    def add_object(self, obj: POOL_ITEM_TYPE) -> bool:
        self._active_objects.discard(obj)
        return self._idle_objects.add_object(obj)

    # return true means truly deleted, otherwise false
    def remove_object(self, obj: POOL_ITEM_TYPE) -> bool:
        remove_from_idle_objects = self._idle_objects.remove_object(obj)

        len_of_active_objects = len(self._active_objects)
        self._active_objects.discard(obj)

        return remove_from_idle_objects or len(self._active_objects) != len_of_active_objects

    def borrow_object(self) -> POOL_ITEM_TYPE:
        obj = self._idle_objects.borrow_object()
        self._active_objects.add(obj)
        return obj

    def return_object(self, obj: POOL_ITEM_TYPE):
        self._active_objects.remove(obj)
        self._idle_objects.return_object(obj)

    def clear(self):
        self._active_objects.clear()
        self._idle_objects.clear()

    def get_num_active(self) -> int:
        return len(self._active_objects)

    def get_active_items(self) -> str:
        return ' | '.join(self._active_objects) if self.get_num_active() > 0 else 'None'

    def get_num_idle(self) -> int:
        return self._idle_objects.get_num()
