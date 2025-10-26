CREATE TABlE @DestinationTable 
(
    CountryID INTEGER,
    CountryName STRING,
    FormalName STRING,
    IsoAlpha3Code STRING,
    IsoNumericCode INTEGER,
    CountryType STRING,
    LatestRecordedPopulation INTEGER,
    Continent STRING,
    Region STRING,
    Subregion STRING,
    Border BYTES,
    LastEditedBy INTEGER,
    ValidFrom DATETIME,
    ValidTo DATETIME,
    LoadDate DATETIME
)