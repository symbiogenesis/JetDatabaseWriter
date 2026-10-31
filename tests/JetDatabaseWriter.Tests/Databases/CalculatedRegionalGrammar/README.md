# Native calculated regional grammar

Microsoft DAO.DBEngine.120 16.0, Access Runtime 16.0.20527.20112, authored these
controls independently on an en-US Windows user profile on 2026-10-30. The
source database was then compacted by the same native engine. Library output
was not used to approve either specimen.

Reproduce with scripts/new-native-calculated-regional-grammar.ps1 in an
en-US user profile; scripts/probe-calculated-native-profile.ps1 also captures
these grammar categories across six independently configured regional profiles.

Native table calculations accept punctuated English month names, including
Day("Jan. 4, 2020") = 4 and Year("4 Sept. 2020") = 2020. A numeric dot date
refuses conversion on this profile. Logical operands parse currency and
parenthesized numeric strings before rounded Boolean conversion. A Date beside
Text compares its regional formatted text: noon equals "12:00:00 PM", differs
from "0.5" and "12:00:00", and orders before "bad". A Date beside a number
instead retains its OLE serial comparison. Every cache survives native compact.

| File | SHA-256 |
| --- | --- |
| native-grammar.accdb | F66DF2125AC5617EA31B16BC7CD0D676B35072A0D2318EB8C6D7B5094B3C39C6 |
| native-grammar-compacted.accdb | ABC4A13E10C8D9414E1A3FA59C6CE216DDF9D08DDCBDB6F5717F68BDEE296BA2 |
