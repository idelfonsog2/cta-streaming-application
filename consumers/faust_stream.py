"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

@dataclass
class Station(faust.Record, validation=True):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


@dataclass
class TransformedStation(faust.Record, validation=True):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect-stations", value_type=Station)
out_topic = app.topic("com.my_cta_server.stations.transformed_station", partitions=1)

transformed_station_table = app.Table(
   "transformed_station_table",
   default=str,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def process(stations):
    async for station in stations:
        color = ""
        if(station.red == True):
            color = "red"
        if (station.blue == True):
            color = "blue"
        if (station.green == True):
            color = "green"

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=color)
        transformed_station_table[station.station_id] = transformed_station
        await out_topic.send(key=transformed_station_table.station_id, value=transformed_station)


if __name__ == "__main__":
    app.main()
