# Third-Party Notices

JetDatabaseWriter incorporates material from the following third-party sources.

## Apache Jackcess

The General Legacy text index sort-key tables embedded in this assembly
(`Internal/IndexCodeTables/index_codes_genleg.txt.gz` and
`Internal/IndexCodeTables/index_codes_ext_genleg.txt.gz`) are taken verbatim
from the Apache Jackcess project resource files
`com/healthmarketscience/jackcess/index_codes_genleg.txt` and
`index_codes_ext_genleg.txt`.

Source: https://github.com/jahlborn/jackcess

The character-handler state machine in
`JetDatabaseWriter/Internal/GeneralLegacyTextIndexEncoder.cs` is a C# port of
`com.healthmarketscience.jackcess.impl.GeneralLegacyIndexCodes` from the same
project.

```
Copyright (c) 2008 Health Market Science, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## OpenMcdf (test fixtures)

The Compound File Binary test fixtures under
`JetDatabaseWriter.Tests/Cfb/` (`TestStream_v*.cfs`,
`MultipleStorage*.cfs`, `FatChainLoop_v3.cfs`,
`LibreOfficeBlankSample_v25.8.*`, `Office365BlankSample_v2507.*`,
`VSPro_v17.suo`) are taken verbatim from the OpenMcdf project's
`OpenMcdf.Tests` directory. They are used solely to validate this
library's CFB reader/writer against a known corpus.

Source: https://github.com/openmcdf/openmcdf

OpenMcdf is licensed under the Mozilla Public License, version 2.0
(MPL-2.0). The full text is available at
https://www.mozilla.org/MPL/2.0/.
