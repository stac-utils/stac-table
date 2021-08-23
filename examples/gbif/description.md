The [Global Biodiversity Information Facility](https://www.gbif.org) (GBIF) is an international network and data infrastructure funded by the world's governments providing global data that document the occurrence of species.

GBIF currently integrates datasets documenting over 1.6 billion species occurrences illustrated here.

![GBIF world map](https://labs.gbif.org/~mblissett/2019/10/analytics-maps/world-2021-01-01.png)

The GBIF occurrence dataset combines data from a wide array of sources including specimen-related data from natural history museums, observations from citizen science networks and environment recording schemes. While these data are constantly changing at [GBIF.org](https://www.gbif.org), periodic snapshots are taken and made available here.

A [post on the GBIF data blog](https://data-blog.gbif.org/post/microsoft-azure-and-gbif/) demonstrates how to work with this data on Azure via Apache Spark.

## Storage resources

Data are stored in [Parquet](https://parquet.apache.org/) files in Azure Blob Storage in the West Europe Azure region, in the following blob container:

`https://ai4edataeuwest.blob.core.windows.net/gbif`

Within that container, the periodic occurrence snapshots are stored in `occurrence/YYYY-MM-DD`, where `YYYY-MM-DD` corresponds to the date of the snapshot.

The snapshot includes all CC-BY licensed data published through GBIF that have coordinates which passed automated quality checks.

Each snapshot contains a `citation.txt` with instructions on how best to cite the data, and the data files themselves in Parquet format: `occurrence.parquet/*`.

Therefore, the data files for the first snapshot are at

`https://ai4edataeuwest.blob.core.windows.net/gbif/occurrence/2021-04-13/occurrence.parquet/*`

and the citation information is at

`https://ai4edataeuwest.blob.core.windows.net/gbif/occurrence/2021-04-13/citation.txt`

The Parquet file schema is described below.  Most field names correspond to [terms from the Darwin Core standard](https://dwc.tdwg.org/terms/), and have been interpreted by GBIF's systems to align taxonomy, location, dates etc.
Additional information may be retrived using the [GBIF API](https://www.gbif.org/developer/summary).

## License and attribution

This dataset is available under a [CC-BY](https://creativecommons.org/licenses/by/4.0/) license and with the GBIF [terms of use](https://www.gbif.org/terms).

Please refer to the GBIF [citation guidelines](https://www.gbif.org/citation-guidelines). For analyses using the whole dataset please use the following citation:

> GBIF.org ([Date]) GBIF Occurrence Data [DOI of dataset]

For analyses where data are significantly filtered, please track the datasetKeys used and use a “[derived dataset](https://www.gbif.org/citation-guidelines#derivedDatasets)” record for citing the data.


## Contact

For questions about this dataset, contact [`aiforearthdatasets@microsoft.com`](mailto:aiforearthdatasets@microsoft.com?subject=gbif%20question) or [`helpdesk@gbif.org`](mailto:helpdesk@gbif.org?subject=MS%20AI4Earth%20question).


## Learn more

The [GBIF data blog](https://data-blog.gbif.org/categories/gbif/) contains a number of articles that can help you analyze GBIF data.