# Third-Party Notices

JetDatabaseWriter incorporates material from the following third-party sources.
It also documents reference-only projects used to guide interoperability and
coverage work where no source code, binaries, or fixtures were copied.

## Sep

The internal delimited-text parser under `JetDatabaseWriter/DelimitedText/`
and its focused test coverage are adapted from concepts, terminology, and edge
case coverage in Sep, including separator/header/row/column terminology,
quoted rows spanning multiple line endings, no-`Peek` reader behavior, and
deterministic fuzz-style parser coverage. JetDatabaseWriter does not depend on
the Sep package.

Source: https://github.com/nietras/Sep

```
MIT License

Copyright (c) 2023 nietras

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Apache Jackcess

The General Legacy text index sort-key tables embedded in this assembly
(`Indexes/CodeTables/index_codes_genleg.txt.gz` and
`Indexes/CodeTables/index_codes_ext_genleg.txt.gz`) are taken verbatim
from the Apache Jackcess project resource files
`com/healthmarketscience/jackcess/index_codes_genleg.txt` and
`index_codes_ext_genleg.txt`.

Source: https://github.com/jahlborn/jackcess

The character-handler state machine in
`JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs` is a C# port of
`com.healthmarketscience.jackcess.impl.GeneralLegacyIndexCodes` from the same
project.

Calculated-column expression operator and built-in function behavior in
`JetDatabaseWriter/Schema/Expressions/CalculatedExpressionEvaluator.cs` is
translated and adapted from Jackcess's `Expressionator` and `Default*Functions`
classes, with storage-specific behavior implemented locally for this library.

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

## Microsoft Extensible Storage Engine (reference only)

The validation matrix uses Microsoft's Extensible Storage Engine repository as
a reference checklist for analogous storage engine risk categories such as page
mutation invariants, delete/replace scrubbing behavior, transaction durability,
cache behavior, and long-value cleanup. JetDatabaseWriter does not incorporate
ESE source code, binaries, or fixtures, and ESE is not used as an Access
MDB/ACCDB file-format oracle.

Source: https://github.com/microsoft/Extensible-Storage-Engine

Microsoft Extensible Storage Engine is licensed under the MIT License. The full
text is available at
https://github.com/microsoft/Extensible-Storage-Engine/blob/master/LICENSE.

Copyright (c) Microsoft Corporation.
