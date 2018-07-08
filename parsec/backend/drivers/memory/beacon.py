from collections import defaultdict

from parsec.backend.beacon import BaseBeaconComponent


class MemoryBeaconComponent(BaseBeaconComponent):
    def __init__(self, signal_ns):
        self._signal_beacon_updated = signal_ns.signal("beacon.updated")
        self.beacons = defaultdict(list)

    async def read(self, id, from_index):
        return self.beacons[id][from_index:]

    async def update(self, id, data, author="anonymous"):
        self.beacons[id].append(data)
        index = len(self.beacons[id])
        self._signal_beacon_updated.send(None, author=author, beacon_id=id, index=index)
