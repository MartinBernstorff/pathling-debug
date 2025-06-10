from pathlib import Path

from pathling import Expression as exp  # type: ignore
from pathling import PathlingContext  # type: ignore

if __name__ == "__main__":
    pc = PathlingContext.create()

    spark_df = pc.read.ndjson(str(Path(__file__).parent / "test-data"))  # type: ignore

    result = (
        spark_df.extract(
            "Appointment",
            columns=[
                exp(
                    "Appointment.participant.actor.resolve().ofType(Patient).address.where(use = 'home').extension('http://hl7.dk/fhir/core/StructureDefinition/dk-core-municipalityCodes').valueCodeableConcept.coding.code",
                    "municipalityCode",
                ),
            ],
        )
        .groupBy("municipalityCode")
        .agg({"*": "count"})
        .show()
    )

    pass
