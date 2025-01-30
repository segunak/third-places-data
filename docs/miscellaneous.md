
# Miscellaneous

Stuff and thangs that don't fit into any specific category related to the project.

## Parking Notes

Trying to make the Parking Situation field (which could be renamed to just "Parking" as I'm fairly certain most everyone will understand what that means).

### Google Options

{
  "freeParkingLot": boolean,
  "paidParkingLot": boolean,
  "freeStreetParking": boolean,
  "paidStreetParking": boolean,
  "valetParking": boolean,
  "freeGarageParking": boolean,
  "paidGarageParking": boolean
}

### More Options

Free Lot
Paid Lot
Free Street
Paid Street
Free Garage
Paid Garage
Free Time-Limited
Paid Time-Limited
Validated

### My Options

Free
Paid
Paid (Garage)
Paid (Street)
Free (Time Limited)
Free (Small Lot)
Free (Street)
Free (Validated)

### AI Advice

Free Lot
Paid Lot
Free Street
Paid Street
Time-Limited
Validated

### Idea for Multi Select Field

Challenge then becomes mapping values from Google to a multi select field, but it can be done. Parking is a complicated field in the sense that it's hard to have discrete values. Multi select really provides that flexibility. You'd have to update the form users submit and the explanations on the website of the Parking Situation field, but if you want the site to really provide value, this is worth the effort.

- Free
- Paid
- Street
- Lot
- Garage
- Time-Limited
- Validated
- Limited
- Plentiful
