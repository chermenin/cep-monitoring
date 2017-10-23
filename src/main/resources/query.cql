--- Generate temperature warnings

from every
    e1 = tempEvents[temperature > ${TEMPERATURE_THRESHOLD}] ->
    e2 = tempEvents[temperature > ${TEMPERATURE_THRESHOLD} and rackID == e1.rackID]
within 10 seconds
select e1.rackID, (e1.temperature + e2.temperature) / 2 as temperature
insert into tempWarnings;

--- Generate temperature alerts

from every
    e1 = tempWarnings ->
    e2 = tempWarnings[rackID == e1.rackID and temperature > e1.temperature]
within 20 seconds
select e1.rackID
insert into tempAlerts;

--- Generate pressure warnings

from every
    e1 = presEvents[pressure > ${PRESSURE_THRESHOLD}] ->
    e2 = presEvents[pressure > ${PRESSURE_THRESHOLD} and sensorID == e1.sensorID]
within 10 seconds
select e1.sensorID, math:max(e1.pressure, e2.pressure) as pressure
insert into presWarnings;

--- Generate temperature alerts

from every
    e1 = presWarnings ->
    e2 = presWarnings[sensorID == e1.sensorID and pressure > e1.pressure]
within 20 seconds
select e1.sensorID
insert into presAlerts;

--- Generate panic alerts

from every
    t = tempAlerts,
    p = presAlerts
within 5 seconds
select t.rackID, p.sensorID
insert into output;
