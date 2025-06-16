from pathlib import Path

from pathling import Expression as exp  # type: ignore
from pathling import PathlingContext  # type: ignore

if __name__ == "__main__":
    pc = PathlingContext.create(enable_extensions=True)

    spark_df = pc.read.ndjson(str(Path(__file__).parent / "test-data"))  # type: ignore

    result = (
        spark_df.extract(
            "Patient",
            columns=[
                exp(
                    "Patient.address.where(use = 'home').extension('http://ehealth.sundhed.dk/fhir/StructureDefinition/ehealth-municipality-code').valueCoding.code",
                    "municipalityCode",
                ),
            ],
        )
        .groupBy("municipalityCode")
        .agg({"*": "count"})
        .sort("count(1)", ascending=False)
    )

    pass
