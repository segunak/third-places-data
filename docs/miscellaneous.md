
# Miscellaneous

Stuff and thangs that don't fit into any specific category related to the project.

## Parking Notes

The details behind the `Parking` field.

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

### My Approach

The `Parking` field, by way of Airtable automation script validation, must begin with one of these three.

* Free
* Paid
* Unsure

After that, any of these modifiers can be added.

* Garage
* Street
* Metered
* Limited
* Plentiful
* Time Limited
* Validation Available
