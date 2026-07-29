# MusicBrainz mbdump Canonical Data Structure (Archive-Derived)

This document is derived directly from the files inside mbdump.tar.bz2.

- Generated (UTC): 2026-07-24T23:06:04Z
- Archive: /mnt/usbc1/MUSIC_MASTER_METADATA/mbdump.tar.bz2
- Total archive members: 182
- mbdump data files: 177
- Non-mbdump members: 5
- Sampled rows per file: 50
- Files with variable sampled field counts: 0
- Empty sampled files: 0

## Non-mbdump Members

- TIMESTAMP (file=True, dir=False, size=30)
- COPYING (file=True, dir=False, size=6390)
- README (file=True, dir=False, size=213)
- REPLICATION_SEQUENCE (file=True, dir=False, size=7)
- SCHEMA_SEQUENCE (file=True, dir=False, size=3)

## Canonical mbdump File Dictionary

### alternative_release_type

- Size bytes: 187
- Sampled rows: 3
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Translation', '\\N', '0', '', '4f2f21db-7b82-3fea-8ac9-7c05fd1eb9de']
- Sample row 2 (first 12 fields): ['2', 'Official translation', '1', '0', '', '25b3e6d9-7288-3355-9f44-e4534a564945']
- Sample row 3 (first 12 fields): ['3', 'Exactly as on cover', '\\N', '1', '', '62941d64-33b0-347b-8ea6-970ccafc012c']

### area

- Size bytes: 13164033
- Sampled rows: 50
- Field count (first row): 14
- Field count range (sampled): 14..14
- Distinct field counts (sampled): [14]
- Sample row 1 (first 12 fields): ['15449', '2913ad77-cec8-4d2f-98d3-d4aa46ab73bc', 'Greccio', '4', '0', '2013-07-21 22:47:57.660809+00', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['38', '71bbafaa-e825-3e15-8ca9-017dcad1748b', 'Canada', '1', '0', '2013-05-27 13:15:52.179105+00', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['43', '82d5f4d6-aed4-3ff5-81d1-5363ac6e97a7', 'Chile', '1', '0', '2013-05-27 12:52:17.320228+00', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']

### area_alias

- Size bytes: 6569115
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['45', '2621', 'Saigon', '\\N', '0', '2013-05-23 23:14:46.692407+00', '\\N', 'Saigon', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['30', '3821', 'City of Bristol', 'en', '0', '2013-05-21 15:18:03.17643+00', '2', 'Bristol, City of', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['12', '243', 'USSR', '\\N', '0', '2013-05-16 10:20:05.158309+00', '3', 'USSR', '\\N', '\\N', '\\N', '\\N']

### area_alias_type

- Size bytes: 175
- Sampled rows: 3
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Area name', '\\N', '0', '\\N', '0b5b3497-d5d9-34e7-a61b-9a6c18aa7b29']
- Sample row 2 (first 12 fields): ['2', 'Formal name', '\\N', '0', '\\N', 'b280c712-f676-342e-a8f2-e5c5fe0159b4']
- Sample row 3 (first 12 fields): ['3', 'Search hint', '\\N', '0', '\\N', '7090dd35-e32e-3422-8a48-224821c2468b']

### area_gid_redirect

- Size bytes: 13923
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['adc0e5a8-4303-4a7c-a09a-7269b39a4555', '1813', '2013-05-22 15:00:19.94759+00']
- Sample row 2 (first 12 fields): ['f1443c80-96e8-4bbf-a182-ed4160945a48', '1812', '2013-05-22 15:00:20.697404+00']
- Sample row 3 (first 12 fields): ['e93c3400-785f-4485-a3d2-c90d625b1404', '1821', '2013-05-22 15:00:21.218773+00']

### area_type

- Size bytes: 1870
- Sampled rows: 9
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Country', '\\N', '1', 'Country is used for areas included (or previously included) in ISO 3166-1, e.g. United States.', '06dd0ae4-8c74-30bb-b43d-95dcedf961de']
- Sample row 2 (first 12 fields): ['2', 'Subdivision', '\\N', '2', 'Subdivision is used for the main administrative divisions of a country, e.g. California, Ontario, Okinawa. These are considered when displaying the parent areas for a given area.', 'fd3d44c5-80a1-3842-9745-2c4972d35afa']
- Sample row 3 (first 12 fields): ['7', 'County', '\\N', '7', 'County is used for smaller administrative divisions of a country which are not the main administrative divisions but are also not municipalities, e.g. counties in the USA. These are not considered when displaying the parent areas for a given area.', 'bcecec27-8bdb-3e00-8254-d948dda502fa']

### artist

- Size bytes: 427149464
- Sampled rows: 50
- Field count (first row): 19
- Field count range (sampled): 19..19
- Distinct field counts (sampled): [19]
- Sample row 1 (first 12 fields): ['2252039', 'fadeb38c-833f-40bc-9d8c-a6383b38b1be', 'Доктор Сатана', 'Доктор Сатана', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['371203', '49add228-eac5-4de8-836c-d75cde7369c3', 'Pete Moutso', 'Moutso, Pete', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '1', '\\N']
- Sample row 3 (first 12 fields): ['3087346', 'dfdce491-133d-4e9f-9e48-795587e181b0', 'UNlT', 'UNlT', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']

### artist_alias

- Size bytes: 55968172
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['125531', '932664', 'Jared Kotler', 'en_US', '0', '2012-10-22 21:00:20.526211+00', '2', 'Kotler, Jared', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['503290', '157784', 'Kwanza Posse', '\\N', '0', '2025-04-22 12:00:13.599079+00', '\\N', 'Kwanza Posse', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['125438', '586849', 'Abraham Gilbert Stein', '\\N', '0', '2012-10-06 00:39:38.38725+00', '2', 'Stein, Abraham Gilbert', '\\N', '\\N', '\\N', '\\N']

### artist_alias_type

- Size bytes: 176
- Sampled rows: 3
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Artist name', '\\N', '0', '\\N', '894afba6-2816-3c24-8072-eadb66bd04bc']
- Sample row 2 (first 12 fields): ['2', 'Legal name', '\\N', '0', '\\N', 'd4dcd0c0-b341-3612-a332-c0ce797b25cf']
- Sample row 3 (first 12 fields): ['3', 'Search hint', '\\N', '0', '\\N', '1937e404-b981-3cb7-8151-4c86ebfc8d8e']

### artist_credit

- Size bytes: 406147651
- Sampled rows: 50
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['4229350', 'Jean-Paul Fouchécourt, Yvonne Naef, Saito Kinen Orchestra, Seiji Ozawa', '4', '1', '2024-12-20 06:18:14.699053+00', '0', '25966362-45fb-4457-88c7-b0d9b06f28e6']
- Sample row 2 (first 12 fields): ['3320885', 'The Turns', '1', '1', '2022-06-09 14:46:02.184524+00', '0', '490c3930-5600-4796-9da6-b05be0a42f66']
- Sample row 3 (first 12 fields): ['3320887', 'Son.Sine', '1', '1', '2022-06-09 14:46:02.184524+00', '0', '9d4b38cf-78f5-4d83-b561-353bec3f9856']

### artist_credit_gid_redirect

- Size bytes: 5352254
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['4c605446-a17d-3f66-aaf7-c799b601dc7c', '3852323', '2024-01-20 16:00:22.012749+00']
- Sample row 2 (first 12 fields): ['fda8e0e3-a8ad-4110-8cc1-098037eb8a84', '3329486', '2022-06-20 13:00:27.610373+00']
- Sample row 3 (first 12 fields): ['da1e7b6a-d2c5-46a1-9acd-f9d811c970c6', '3329487', '2022-06-20 13:01:13.517018+00']

### artist_credit_name

- Size bytes: 232454167
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['578352', '0', '578352', 'Gustav Ruppke', '']
- Sample row 2 (first 12 fields): ['273232', '0', '273232', 'Zachary', '']
- Sample row 3 (first 12 fields): ['153193', '0', '153193', 'The High Level Ranters', '']

### artist_gid_redirect

- Size bytes: 6503299
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['4502175e-c649-4614-85ff-7bf578cd8ef3', '535933', '2011-05-17 08:00:05.775423+00']
- Sample row 2 (first 12 fields): ['224efe8c-c070-4e8a-9402-d41d52f5b4f1', '132084', '2011-05-16 14:57:06.530063+00']
- Sample row 3 (first 12 fields): ['dbe7b07c-cdcc-41b9-bae3-510cc5ef6248', '39192', '2011-05-16 14:57:06.530063+00']

### artist_ipi

- Size bytes: 5843079
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['732258', '00244121213', '0', '2012-05-15 19:04:48.684349+00']
- Sample row 2 (first 12 fields): ['206257', '00134824187', '0', '2017-12-10 21:00:16.526169+00']
- Sample row 3 (first 12 fields): ['49961', '00488938380', '0', '2012-05-15 19:04:48.684349+00']

### artist_isni

- Size bytes: 8632415
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['2080164', '0000000063727363', '0', '2020-11-27 17:10:03.929711+00']
- Sample row 2 (first 12 fields): ['381880', '0000000001212073', '0', '2020-12-02 14:01:04.718654+00']
- Sample row 3 (first 12 fields): ['533929', '0000000044053818', '0', '2013-05-15 16:00:07.394777+00']

### artist_type

- Size bytes: 1129
- Sampled rows: 6
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['3', 'Other', '\\N', '99', '\\N', 'ac897045-5043-3294-969b-187360e45d86']
- Sample row 2 (first 12 fields): ['2', 'Group', '\\N', '2', 'A grouping of multiple musicians who perform together (in some cases, some or all of the members might differ in different performances or recordings).', 'e431f5f6-b5d2-343d-8b36-72607fffb74b']
- Sample row 3 (first 12 fields): ['1', 'Person', '\\N', '1', 'This indicates an individual person, be it under its legal name (“John Lennon”), or a performance name (“Sting”).', 'b6e035f4-3ce9-331c-97df-83397230b0df']

### cdtoc

- Size bytes: 201085856
- Sampled rows: 50
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['1', 'borOdvYNUkc2SF8GrzPepad0H3M-', '0f082b02', '2', '157005', '{150,77950}', '2011-05-16 14:57:06.530063+00']
- Sample row 2 (first 12 fields): ['2', 'gtWBI_F_fQFSSRt8nVChAVFaT_A-', '670d1e09', '9', '252000', '{150,31615,67600,87137,108242,127110,142910,166340,231445}', '2011-05-16 14:57:06.530063+00']
- Sample row 3 (first 12 fields): ['3', 'IEV_Eb5RtCtuejxOpuJYMdfixiw-', '7d0aee0b', '11', '210070', '{150,25277,34987,60880,78090,91067,112195,130450,150412,170305,185307}', '2011-05-16 14:57:06.530063+00']

### country_area

- Size bytes: 926
- Sampled rows: 50
- Field count (first row): 1
- Field count range (sampled): 1..1
- Distinct field counts (sampled): [1]
- Sample row 1 (first 12 fields): ['1']
- Sample row 2 (first 12 fields): ['2']
- Sample row 3 (first 12 fields): ['3']

### editor_collection_type

- Size bytes: 1219
- Sampled rows: 17
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['2', 'Owned music', 'release', '1', '1', '\\N', 'c26c6ec4-17f0-32de-95c5-fa724dbdb308']
- Sample row 2 (first 12 fields): ['3', 'Wishlist', 'release', '1', '2', '\\N', '5feda0f8-14cf-38fd-89d0-ac2d48ad3324']
- Sample row 3 (first 12 fields): ['5', 'Attending', 'event', '4', '1', '\\N', 'de6aedf5-73c2-3f7c-88f8-e128c189a205']

### event

- Size bytes: 38502515
- Sampled rows: 50
- Field count (first row): 17
- Field count range (sampled): 17..17
- Distinct field counts (sampled): [17]
- Sample row 1 (first 12 fields): ['1607', 'ebe6ce0f-22c0-4fe7-bfd4-7a0397c9fe94', 'Taubertal-Festival 2004, Day 1', '2004', '8', '13', '2004', '8', '13', '\\N', '2', 'f']
- Sample row 2 (first 12 fields): ['20', '5774273c-bb54-480e-89c3-1f71326402ed', 'Shakespeare Company Berlin at Ilmbühne', '2014', '8', '15', '2014', '8', '15', '\\N', '1', 'f']
- Sample row 3 (first 12 fields): ['1595', '14084715-6306-4126-9062-865080e6db2d', '「Kaléidoscope—天使の狂宴—」@ Zepp Osaka', '2007', '8', '3', '2007', '8', '3', '\\N', '1', 'f']

### event_alias

- Size bytes: 337794
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '139', 'コミックマーケット72', 'ja', '0', '2014-11-17 22:57:58.021111+00', '\\N', 'コミックマーケット72', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['2', '139', 'Comic Market 72', 'en', '0', '2014-11-17 22:58:07.578304+00', '\\N', 'Comic Market 72', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['117', '195', 'M3-29', '\\N', '0', '2014-11-17 23:43:28.989159+00', '\\N', 'M3-29', '\\N', '\\N', '\\N', '\\N']

### event_alias_type

- Size bytes: 117
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Event name', '\\N', '0', '\\N', '412aac48-424b-3052-a314-1f926e8018c8']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '9b7e72d0-ef3f-3c75-908c-f94c48eb6484']

### event_gid_redirect

- Size bytes: 63123
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['68becc69-ca18-4bb5-839a-3a3d99c25665', '47', '2014-11-18 15:02:03.254354+00']
- Sample row 2 (first 12 fields): ['fe60af76-f22a-465d-bca2-6e6f716ddbec', '167', '2014-11-18 15:02:03.458306+00']
- Sample row 3 (first 12 fields): ['a1281e87-21a8-4670-8fab-7be496eb41d7', '79', '2014-11-18 16:01:52.845517+00']

### event_type

- Size bytes: 1760
- Sampled rows: 8
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Concert', '\\N', '1', 'An individual concert by a single artist or collaboration, often with supporting artists who perform before the main act.', 'ef55e8d7-3d00-394a-8012-f5506a29ff0b']
- Sample row 2 (first 12 fields): ['2', 'Festival', '\\N', '2', 'An event where a number of different acts perform across the course of the day. Larger festivals may be spread across multiple days.', 'b6ded574-b592-3f0e-b56e-5b5f06aa0678']
- Sample row 3 (first 12 fields): ['40', 'Competition', '\\N', '8', 'An event in which the participants perform live and a winner is then chosen (as opposed to an award ceremony where awards are presented for previously released or performed music).', '42656500-7573-4b56-8934-af6fa2a305ec']

### gender

- Size bytes: 362
- Sampled rows: 5
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['2', 'Female', '\\N', '2', '\\N', '93452b5a-a947-30c8-934f-6a4056b151c2']
- Sample row 2 (first 12 fields): ['1', 'Male', '\\N', '1', '\\N', '36d3d30a-839d-3eda-8cb3-29be4384e4a9']
- Sample row 3 (first 12 fields): ['4', 'Not applicable', '\\N', '4', "For cases where gender just doesn't apply at all (like companies entered as artists).", '8cf3c8c8-4af9-4b53-bad4-e43c0450ba04']

### genre

- Size bytes: 186565
- Sampled rows: 50
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', '54c01942-22fd-4184-9877-1db0089b18f1', 'acid house', '', '0', '2019-05-13 17:46:28.122726+00']
- Sample row 2 (first 12 fields): ['2', '7dc2b20f-3953-4874-b9bf-41b8ba06d20c', 'acid jazz', '', '0', '2019-05-13 17:46:28.122726+00']
- Sample row 3 (first 12 fields): ['3', 'ba64013e-27bb-4f14-a530-8d25b296e0da', 'acid techno', '', '0', '2019-05-13 17:46:28.122726+00']

### genre_alias

- Size bytes: 87464
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '28', 'avant-pop', 'en', '0', '2022-06-29 17:11:03.432655+00', '1', 'avant-pop', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['2', '27', 'avant-metal', 'en', '0', '2022-06-29 17:11:57.064054+00', '1', 'avant-metal', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['3', '26', 'avant-jazz', 'en', '0', '2022-06-29 17:14:56.75658+00', '1', 'avant-jazz', '\\N', '\\N', '\\N', '\\N']

### genre_alias_type

- Size bytes: 117
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Genre name', '\\N', '0', '\\N', '61e89fea-acce-3908-a590-d999dc627ac9']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '5d81fc72-598a-3a9d-a85a-a471c6ba84dc']

### instrument

- Size bytes: 224151
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['687', 'c1dbb66d-2356-417a-81ad-f688fee33257', 'guitarrón mexicano', '2', '0', '2015-02-15 07:32:32.570132+00', '', 'The guitarrón mexicano is a very large and deep-bodied Mexican guitar-like instrument with six strings which is traditionally played in mariachi groups.']
- Sample row 2 (first 12 fields): ['695', '2474c241-d267-433a-a404-688b13c51d11', 'jouhikko', '2', '0', '2015-02-25 19:47:04.610414+00', '', 'The jouhikko is a traditional, 2 or 3 stringed bowed lyre, from Finland and Karelia.']
- Sample row 3 (first 12 fields): ['701', 'c0cc863c-ea65-4b8a-b365-28b81b72d846', 'friction idiophone', '3', '0', '2015-02-26 10:28:49.766548+00', '', 'Friction idiophones are idiophones where the sound is created by the instrument being rubbed.']

### instrument_alias

- Size bytes: 1077581
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['315', '492', '대금', 'ko', '0', '2014-05-25 18:54:00.090278+00', '1', '대금', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['110', '224', '马头琴', 'zh_Hans', '0', '2014-05-26 02:24:32.130018+00', '1', '马头琴', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['382', '550', '小鑼', 'zh_Hant', '0', '2014-05-26 02:27:26.602921+00', '1', '小鑼', '\\N', '\\N', '\\N', '\\N']

### instrument_alias_type

- Size bytes: 180
- Sampled rows: 3
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Instrument name', '\\N', '0', '\\N', '2322fc94-fbf3-3c09-b23c-aa5ec8d14fcd']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '7d5ef40f-4856-3000-8667-aa13b9db547d']
- Sample row 3 (first 12 fields): ['3', 'Brand name', '\\N', '1', '\\N', '9a75677e-768a-3307-808c-a08c3d8ecd68']

### instrument_gid_redirect

- Size bytes: 1764
- Sampled rows: 25
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['dd91b67e-014f-4f12-b995-463bc7f54609', '13', '2014-10-02 10:04:00.051532+00']
- Sample row 2 (first 12 fields): ['4c8eff90-b3cf-4a8f-be1f-2aeb1ddc3fad', '205', '2015-02-26 19:04:39.128728+00']
- Sample row 3 (first 12 fields): ['47765c7c-2d9b-40a9-b3be-8d88d29fcfd1', '46', '2015-02-26 20:07:00.306796+00']

### instrument_type

- Size bytes: 608
- Sampled rows: 7
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Wind instrument', '\\N', '1', '\\N', '876464a8-e74f-3f40-9bd3-637d2b1743ae']
- Sample row 2 (first 12 fields): ['2', 'String instrument', '\\N', '2', '\\N', 'cc00f97f-cf3d-3ae2-9163-041cb1a0d726']
- Sample row 3 (first 12 fields): ['3', 'Percussion instrument', '\\N', '3', '\\N', '68fa2525-4c23-3386-bb81-e84994342e9a']

### iso_3166_1

- Size bytes: 1700
- Sampled rows: 50
- Field count (first row): 2
- Field count range (sampled): 2..2
- Distinct field counts (sampled): [2]
- Sample row 1 (first 12 fields): ['1', 'AF']
- Sample row 2 (first 12 fields): ['2', 'AL']
- Sample row 3 (first 12 fields): ['3', 'DZ']

### iso_3166_2

- Size bytes: 53532
- Sampled rows: 50
- Field count (first row): 2
- Field count range (sampled): 2..2
- Distinct field counts (sampled): [2]
- Sample row 1 (first 12 fields): ['261', 'US-MD']
- Sample row 2 (first 12 fields): ['262', 'US-AK']
- Sample row 3 (first 12 fields): ['263', 'US-AL']

### iso_3166_3

- Size bytes: 84
- Sampled rows: 9
- Field count (first row): 2
- Field count range (sampled): 2..2
- Distinct field counts (sampled): [2]
- Sample row 1 (first 12 fields): ['244', 'DDDE']
- Sample row 2 (first 12 fields): ['245', 'CSHH']
- Sample row 3 (first 12 fields): ['235', 'YUCS']

### isrc

- Size bytes: 373170358
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['75478', '10', 'GBAAA9000038', '0', '2011-05-16 16:08:20.288158+00']
- Sample row 2 (first 12 fields): ['75476', '11', 'GBAAA9100082', '0', '2011-05-16 16:08:20.288158+00']
- Sample row 3 (first 12 fields): ['75475', '14', 'GBAAA9100044', '0', '2011-05-16 16:08:20.288158+00']

### iswc

- Size bytes: 33346258
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['1', '4706957', 'T-101.724.790-2', '0', '2012-05-15 19:02:33.036415+00']
- Sample row 2 (first 12 fields): ['2', '12432360', 'T-007.135.098-5', '0', '2012-05-15 19:02:33.036415+00']
- Sample row 3 (first 12 fields): ['3', '12431967', 'T-801.633.170-8', '0', '2012-05-15 19:02:33.036415+00']

### l_area_area

- Size bytes: 7219802
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['2', '118734', '222', '262', '0', '2013-05-17 20:06:26.445481+00', '0', '', '']
- Sample row 2 (first 12 fields): ['80', '118734', '81', '339', '0', '2013-05-17 21:36:09.153766+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '118734', '222', '263', '0', '2013-05-17 20:06:54.821594+00', '0', '', '']

### l_area_event

- Size bytes: 468925
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['6486', '199639', '15973', '64003', '0', '2022-11-12 22:21:11.078033+00', '0', '', '']
- Sample row 2 (first 12 fields): ['75', '199639', '1178', '1699', '0', '2014-12-13 00:15:24.441231+00', '0', '', '']
- Sample row 3 (first 12 fields): ['1', '199639', '5229', '665', '0', '2014-11-18 14:04:30.155858+00', '0', '', '']

### l_area_genre

- Size bytes: 72354
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '927355', '134', '439', '0', '2022-07-07 07:30:49.006653+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '927355', '87', '439', '0', '2022-07-07 07:30:51.068434+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '927355', '3', '337', '0', '2022-07-07 08:58:01.407158+00', '0', '', '']

### l_area_instrument

- Size bytes: 34786
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '183557', '431', '245', '0', '2014-08-04 20:10:02.875694+00', '0', '', '']
- Sample row 2 (first 12 fields): ['37', '183557', '44', '546', '0', '2014-08-04 20:19:25.474048+00', '0', '', '']
- Sample row 3 (first 12 fields): ['2', '183557', '107', '616', '0', '2014-08-04 20:10:08.8049+00', '0', '', '']

### l_area_label

- Size bytes: 26529
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['288', '1079912', '52874', '354980', '0', '2026-03-12 18:32:19.576822+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '1079912', '1178', '388', '0', '2024-02-26 10:22:02.232953+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '1079912', '9645', '2373', '0', '2024-02-26 14:14:05.517509+00', '0', '', '']

### l_area_recording

- Size bytes: 12234935
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '133746', '5180', '13790324', '0', '2013-10-17 15:06:40.811408+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '133746', '5180', '13790323', '0', '2013-10-17 15:06:42.803139+00', '0', '', '']
- Sample row 3 (first 12 fields): ['21', '138762', '7703', '15464022', '0', '2013-11-13 23:00:28.111893+00', '0', '', '']

### l_area_release

- Size bytes: 3087964
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '137966', '23037', '1347691', '0', '2013-11-17 11:00:13.574562+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '137967', '4434', '1347691', '0', '2013-11-17 11:00:13.857694+00', '0', '', '']
- Sample row 3 (first 12 fields): ['5207', '208335', '107', '382191', '0', '2017-12-11 11:56:44.724913+00', '0', '', '']

### l_area_series

- Size bytes: 49401
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '921748', '4453', '17338', '0', '2022-06-12 20:06:50.308593+00', '0', '', '']
- Sample row 2 (first 12 fields): ['50', '921748', '107', '17880', '0', '2022-08-08 20:41:25.793447+00', '0', '', '']
- Sample row 3 (first 12 fields): ['51', '921748', '119732', '17674', '0', '2022-08-14 05:59:14.451626+00', '0', '', '']

### l_area_url

- Size bytes: 17133835
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['383628', '118733', '120448', '13342133', '0', '2024-09-15 14:16:22.340101+00', '0', '', '']
- Sample row 2 (first 12 fields): ['381364', '118733', '119201', '7429065', '0', '2020-09-30 22:05:35.096845+00', '0', '', '']
- Sample row 3 (first 12 fields): ['4', '118733', '262', '1712871', '0', '2013-05-17 20:06:17.181549+00', '0', '', '']

### l_area_work

- Size bytes: 491423
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '118444', '107', '12448408', '0', '2013-05-15 15:46:29.412828+00', '0', '', '']
- Sample row 2 (first 12 fields): ['5998', '263751', '1812', '14057353', '0', '2022-12-21 07:21:52.784855+00', '0', '', '']
- Sample row 3 (first 12 fields): ['30', '118444', '25', '12449093', '0', '2013-05-15 16:11:51.550919+00', '0', '', '']

### l_artist_artist

- Size bytes: 53823439
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '6337', '475809', '287770', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 2 (first 12 fields): ['3', '6338', '238828', '3184', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 3 (first 12 fields): ['6', '6337', '367163', '493186', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']

### l_artist_event

- Size bytes: 21599563
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['300484', '199467', '628146', '105412', '0', '2025-08-30 08:36:16.19672+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '199468', '559605', '3', '0', '2014-11-17 21:50:26.032977+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '199467', '1097691', '7', '0', '2014-11-17 22:11:39.0969+00', '0', '', '']

### l_artist_genre

- Size bytes: 55
- Sampled rows: 1
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1117609', '34423', '94', '0', '2024-07-22 10:30:16.505638+00', '0', '', '']

### l_artist_instrument

- Size bytes: 5668
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '311992', '1218252', '121', '0', '2016-08-01 11:22:19.207793+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '312066', '1395197', '19', '0', '2016-08-01 14:14:07.499778+00', '0', '', '']
- Sample row 3 (first 12 fields): ['24', '393478', '1261488', '808', '0', '2017-05-31 08:31:04.355584+00', '0', '', '']

### l_artist_label

- Size bytes: 4106550
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '12132', '473113', '16028', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '12132', '474797', '16278', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '12132', '289759', '16522', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']

### l_artist_place

- Size bytes: 3096161
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '133755', '442612', '774', '0', '2013-10-17 15:34:07.697702+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '133755', '1051543', '774', '0', '2013-10-17 15:34:57.082365+00', '0', '', '']
- Sample row 3 (first 12 fields): ['46', '133953', '999594', '1142', '0', '2013-10-19 03:02:42.95189+00', '0', '', '']

### l_artist_recording

- Size bytes: 1245536429
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['643693', '12735', '4', '274774', '0', '2011-05-16 16:08:20.288158+00', '0', '', '']
- Sample row 2 (first 12 fields): ['5857956', '12737', '761795', '9457764', '0', '2017-05-15 20:53:58.939831+00', '0', '', '']
- Sample row 3 (first 12 fields): ['14758668', '12737', '505993', '9521763', '0', '2023-05-15 18:14:43.004237+00', '0', '', '']

### l_artist_release

- Size bytes: 101202750
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['192081', '46', '289580', '909165', '0', '2011-11-17 03:00:44.621057+00', '0', '', '']
- Sample row 2 (first 12 fields): ['277563', '29', '204', '7922', '0', '2011-05-16 15:59:00.785958+00', '0', '', '']
- Sample row 3 (first 12 fields): ['277564', '29', '204', '280871', '0', '2011-05-16 15:59:00.785958+00', '0', '', '']

### l_artist_release_group

- Size bytes: 1019186
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '47', '162235', '612285', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '101', '471908', '707628', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 3 (first 12 fields): ['4', '47', '469841', '650261', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']

### l_artist_series

- Size bytes: 3060459
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1233', '240641', '30040', '7463', '0', '2017-12-17 17:58:12.052548+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '167394', '1115622', '25', '0', '2014-05-14 23:28:52.535433+00', '0', '', '']
- Sample row 3 (first 12 fields): ['30', '167393', '15747', '226', '0', '2014-05-16 01:41:54.035361+00', '0', '', '']

### l_artist_url

- Size bytes: 408521976
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['219492', '26038', '4', '12092', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']
- Sample row 2 (first 12 fields): ['207305', '26038', '6', '22104', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']
- Sample row 3 (first 12 fields): ['206858', '26038', '7', '22105', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']

### l_artist_work

- Size bytes: 304952443
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['3802771', '12757', '2148099', '14475197', '0', '2024-09-01 07:13:11.086139+00', '0', 'Roberto Sotgia', '']
- Sample row 2 (first 12 fields): ['3802772', '12757', '1023016', '14475197', '0', '2024-09-01 07:13:11.086139+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3802773', '12757', '697643', '14475197', '0', '2024-09-01 07:13:11.086139+00', '0', '', '']

### l_event_event

- Size bytes: 1682963
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['185', '201443', '2516', '2690', '0', '2015-02-21 12:10:53.648427+00', '0', '', '']
- Sample row 2 (first 12 fields): ['188', '201443', '2516', '2693', '0', '2015-02-21 12:11:58.348571+00', '0', '', '']
- Sample row 3 (first 12 fields): ['191', '201443', '2516', '2695', '0', '2015-02-21 12:12:39.593632+00', '0', '', '']

### l_event_label

- Size bytes: 109659
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1033790', '45270', '271727', '0', '2023-07-14 09:53:54.223917+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '1033790', '36618', '271728', '0', '2023-07-14 09:57:23.374294+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '1033790', '71556', '271730', '0', '2023-07-14 10:02:43.878718+00', '0', '', '']

### l_event_place

- Size bytes: 6681360
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1335', '199471', '1594', '8689', '0', '2014-12-10 17:06:03.252513+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '199471', '3', '8164', '0', '2014-11-17 22:04:26.319871+00', '0', '', '']
- Sample row 3 (first 12 fields): ['36', '199471', '38', '6602', '0', '2014-11-17 22:20:46.071654+00', '0', '', '']

### l_event_recording

- Size bytes: 13252469
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1099', '199682', '1602', '17156093', '0', '2014-12-10 22:22:50.386325+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1100', '199682', '1602', '17156094', '0', '2014-12-10 22:22:50.386325+00', '0', '', '']
- Sample row 3 (first 12 fields): ['1101', '199682', '1602', '17156108', '0', '2014-12-10 22:22:50.386325+00', '0', '', '']

### l_event_release

- Size bytes: 1197245
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['651', '199662', '1771', '82102', '0', '2014-12-15 00:16:23.124403+00', '0', '', '']
- Sample row 2 (first 12 fields): ['678', '199662', '1827', '899246', '0', '2014-12-16 00:51:39.457276+00', '0', '', '']
- Sample row 3 (first 12 fields): ['5431', '199662', '3263', '2082563', '0', '2017-12-11 05:54:33.18147+00', '0', '', '']

### l_event_release_group

- Size bytes: 77959
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['15', '199489', '137', '1406953', '0', '2014-11-17 22:51:42.449663+00', '0', '', '']
- Sample row 2 (first 12 fields): ['3', '199489', '125', '1387128', '0', '2014-11-17 22:48:24.3459+00', '0', '', '']
- Sample row 3 (first 12 fields): ['2', '199489', '124', '198948', '0', '2014-11-17 22:48:06.099088+00', '0', '', '']

### l_event_series

- Size bytes: 3512484
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['579', '199484', '1616', '2527', '0', '2014-12-12 00:17:06.270173+00', '1', '', '']
- Sample row 2 (first 12 fields): ['580', '199484', '1617', '2527', '0', '2014-12-12 00:17:06.270173+00', '2', '', '']
- Sample row 3 (first 12 fields): ['7649', '199484', '14539', '4473', '0', '2016-05-02 00:30:36.606211+00', '23', '', '']

### l_event_url

- Size bytes: 5225640
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '199469', '5', '2767185', '0', '2014-11-17 22:01:43.272856+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '199469', '5', '2767186', '0', '2014-11-17 22:01:43.272856+00', '0', '', '']
- Sample row 3 (first 12 fields): ['31', '199490', '155', '2767308', '0', '2014-11-17 23:05:44.317685+00', '0', '', '']

### l_event_work

- Size bytes: 195737
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '217069', '2492', '12732715', '0', '2015-02-15 00:17:46.192559+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '217077', '2493', '12808129', '0', '2015-02-15 01:23:49.672778+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '217186', '2494', '12808402', '0', '2015-02-15 13:37:09.598854+00', '0', '', '']

### l_genre_genre

- Size bytes: 192766
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '944810', '38', '23', '0', '2022-09-16 10:55:42.625971+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '944810', '358', '1391', '0', '2022-09-16 11:03:09.4389+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '944810', '321', '452', '0', '2022-09-16 11:03:23.876694+00', '0', '', '']

### l_genre_instrument

- Size bytes: 13280
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '927805', '346', '55', '0', '2022-07-08 07:59:40.029763+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '927805', '346', '101', '0', '2022-07-08 08:04:30.191876+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '927805', '222', '101', '0', '2022-07-08 08:04:30.191876+00', '0', '', '']

### l_genre_label

- Size bytes: 109
- Sampled rows: 2
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1119909', '268', '330', '0', '2024-08-01 08:56:00.15684+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '1119909', '212', '18697', '0', '2024-08-01 09:04:11.199785+00', '0', '', '']

### l_genre_place

- Size bytes: 56
- Sampled rows: 1
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1119908', '678', '52746', '0', '2024-08-01 08:16:20.925769+00', '0', '', '']

### l_genre_release_group

- Size bytes: 405
- Sampled rows: 7
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1117611', '507', '803533', '0', '2024-07-22 10:38:51.831983+00', '0', '', '']
- Sample row 2 (first 12 fields): ['72', '1117611', '2023', '2814445', '0', '2025-04-08 15:15:04.77171+00', '0', '', '']
- Sample row 3 (first 12 fields): ['73', '1117611', '92', '1160631', '0', '2025-04-24 21:11:07.937799+00', '0', '', '']

### l_genre_url

- Size bytes: 270859
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '925714', '603', '9662120', '0', '2022-06-28 09:40:04.112998+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '925715', '603', '9662121', '0', '2022-06-28 09:40:04.112998+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '925714', '452', '9662283', '0', '2022-06-28 10:36:33.166879+00', '0', '', '']

### l_instrument_instrument

- Size bytes: 75623
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['683', '166958', '35', '684', '0', '2015-02-13 18:48:51.739521+00', '0', '', '']
- Sample row 2 (first 12 fields): ['52', '166958', '18', '290', '0', '2015-02-14 21:29:55.155783+00', '0', '', '']
- Sample row 3 (first 12 fields): ['58', '166958', '18', '296', '0', '2015-02-14 21:29:55.155783+00', '0', '', '']

### l_instrument_label

- Size bytes: 280
- Sampled rows: 5
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '369481', '775', '133898', '0', '2017-03-21 19:04:18.655193+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '369500', '835', '133901', '0', '2017-03-21 20:29:48.763809+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '369508', '836', '133901', '0', '2017-03-21 20:46:27.501453+00', '0', '', '']

### l_instrument_url

- Size bytes: 97133
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['687', '166955', '44', '2516844', '0', '2014-05-14 19:04:50.00155+00', '0', '', '']
- Sample row 2 (first 12 fields): ['702', '166955', '311', '2516873', '0', '2014-05-14 19:05:03.767156+00', '0', '', '']
- Sample row 3 (first 12 fields): ['771', '166955', '390', '2517007', '0', '2014-05-14 19:19:23.245373+00', '0', '', '']

### l_label_label

- Size bytes: 2018628
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '26583', '2740', '16352', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '26584', '2202', '3229', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 3 (first 12 fields): ['5', '26583', '3513', '18776', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']

### l_label_place

- Size bytes: 69272
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['872', '744770', '93546', '71790', '0', '2024-09-22 18:41:56.956608+00', '0', '', '']
- Sample row 2 (first 12 fields): ['873', '744770', '29054', '71837', '0', '2024-09-25 13:35:29.162935+00', '0', '', '']
- Sample row 3 (first 12 fields): ['29', '744770', '126793', '46969', '0', '2020-12-07 22:26:28.977299+00', '0', '', '']

### l_label_recording

- Size bytes: 66004053
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['482843', '665440', '267432', '26450312', '0', '2023-05-15 19:16:44.929824+00', '0', '', '']
- Sample row 2 (first 12 fields): ['858', '26980', '532', '9471798', '0', '2011-05-16 16:08:20.288158+00', '0', '', '']
- Sample row 3 (first 12 fields): ['605', '26980', '532', '9471802', '0', '2011-05-16 16:08:20.288158+00', '0', '', '']

### l_label_release

- Size bytes: 48081095
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['946', '28686', '6788', '1056350', '0', '2011-08-14 11:00:06.362724+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1079', '75616', '72999', '1219780', '0', '2012-11-28 10:00:12.320974+00', '0', '', '']
- Sample row 3 (first 12 fields): ['147934', '135848', '145658', '2856782', '0', '2020-10-24 04:41:27.374137+00', '0', '', '']

### l_label_release_group

- Size bytes: 15748
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '543702', '185', '1847009', '0', '2018-10-27 08:57:10.616855+00', '0', '', '']
- Sample row 2 (first 12 fields): ['76', '543702', '87225', '2306635', '0', '2022-09-14 14:09:07.108284+00', '0', '', '']
- Sample row 3 (first 12 fields): ['667', '543702', '21634', '4600570', '0', '2026-02-01 01:13:35.955617+00', '0', 'FXHE Detroit', '']

### l_label_series

- Size bytes: 509859
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '470774', '388', '575', '0', '2018-02-20 12:32:39.067339+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '470774', '2484', '7772', '0', '2018-02-20 12:54:10.041956+00', '0', '', '']
- Sample row 3 (first 12 fields): ['1186', '470774', '153940', '13067', '0', '2020-11-28 11:35:31.093435+00', '0', '', '']

### l_label_url

- Size bytes: 24430535
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['325407', '27042', '303973', '13270450', '0', '2024-09-01 19:37:35.767551+00', '0', '', '']
- Sample row 2 (first 12 fields): ['8964', '27034', '38', '592329', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']
- Sample row 3 (first 12 fields): ['284626', '49402', '623', '11021195', '0', '2023-05-15 18:10:23.57038+00', '0', '', '']

### l_label_work

- Size bytes: 39263025
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['3846', '26981', '464', '12097183', '0', '2011-05-16 16:27:39.450042+00', '0', '', '']
- Sample row 2 (first 12 fields): ['605', '26981', '532', '9471802', '0', '2011-05-16 16:27:39.450042+00', '0', '', '']
- Sample row 3 (first 12 fields): ['321664', '26981', '168016', '13874033', '0', '2021-11-23 07:42:31.362521+00', '0', '', '']

### l_place_place

- Size bytes: 379631
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['2569', '138113', '6651', '33185', '0', '2019-11-08 12:13:05.297589+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '138113', '1418', '1048', '0', '2013-11-10 22:09:09.072671+00', '0', '', '']
- Sample row 3 (first 12 fields): ['28', '138113', '1215', '2217', '0', '2013-11-10 22:34:20.462642+00', '0', '', '']

### l_place_recording

- Size bytes: 167551776
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1832110', '972489', '3878', '15008226', '0', '2022-12-20 23:46:04.212894+00', '0', 'New Birdland', '']
- Sample row 2 (first 12 fields): ['2963', '134154', '1157', '15382279', '0', '2013-10-19 12:48:45.149924+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3978', '133744', '2692', '15228028', '0', '2013-11-26 22:00:19.471118+00', '0', '', '']

### l_place_release

- Size bytes: 8046813
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['62138', '325053', '7294', '406487', '0', '2021-11-23 07:19:07.356337+00', '0', '', '']
- Sample row 2 (first 12 fields): ['81', '134136', '784', '1236094', '0', '2013-10-19 03:57:20.16084+00', '0', '', '']
- Sample row 3 (first 12 fields): ['69', '133780', '1097', '19202', '0', '2013-10-18 21:34:00.263803+00', '0', '', '']

### l_place_series

- Size bytes: 34037
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['214', '788660', '59032', '19271', '0', '2023-01-02 17:57:46.47543+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '788660', '1995', '13531', '0', '2021-02-18 13:18:46.567866+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '788660', '10260', '13457', '0', '2021-02-18 13:20:05.732967+00', '0', '', '']

### l_place_url

- Size bytes: 7143383
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['16154', '133735', '12049', '3163117', '0', '2015-06-25 15:08:40.582189+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1125', '133743', '934', '2009250', '0', '2013-10-25 02:00:19.628985+00', '0', '', '']
- Sample row 3 (first 12 fields): ['72592', '133743', '52303', '8781333', '0', '2021-11-23 13:39:22.209591+00', '0', '', '']

### l_place_work

- Size bytes: 377853
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['25', '138084', '2420', '12495403', '0', '2013-11-10 20:29:43.548543+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1', '137997', '1321', '3521936', '0', '2013-11-10 15:30:45.775758+00', '0', '', '']
- Sample row 3 (first 12 fields): ['4290', '1128226', '45432', '13497072', '0', '2024-09-02 13:19:22.588037+00', '0', '', '']

### l_recording_recording

- Size bytes: 23104216
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['116968', '27067', '24078698', '20252437', '0', '2019-04-11 01:23:14.167086+00', '0', '', '']
- Sample row 2 (first 12 fields): ['193740', '239089', '32034488', '32056928', '0', '2022-05-16 20:55:17.014061+00', '0', '', '']
- Sample row 3 (first 12 fields): ['108117', '27072', '21803394', '21545907', '0', '2019-05-09 20:00:57.656827+00', '0', '', '']

### l_recording_release

- Size bytes: 62846
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1673', '6296', '37274558', '4248980', '0', '2024-02-07 07:21:24.457545+00', '0', '', '']
- Sample row 2 (first 12 fields): ['67', '6296', '228573', '7077', '0', '2011-05-16 16:08:20.288158+00', '0', '', '']
- Sample row 3 (first 12 fields): ['70', '6296', '1721900', '22403', '0', '2011-05-16 16:08:20.288158+00', '0', '', '']

### l_recording_series

- Size bytes: 2747819
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1636', '176999', '16348172', '1665', '0', '2014-07-13 01:14:06.44717+00', '1', '', '']
- Sample row 2 (first 12 fields): ['3994', '176999', '18170947', '3568', '0', '2015-08-26 13:26:15.299481+00', '1', '', '']
- Sample row 3 (first 12 fields): ['3069', '177000', '16973226', '1975', '0', '2014-10-19 14:50:42.830393+00', '4', '', '']

### l_recording_url

- Size bytes: 273936028
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1488', '27098', '76485', '22957', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1529', '27098', '76486', '22957', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']
- Sample row 3 (first 12 fields): ['1565', '27098', '76489', '22957', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']

### l_recording_work

- Size bytes: 517965750
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['2541720', '27125', '20933682', '9446142', '0', '2017-05-15 20:51:14.569942+00', '0', '', '']
- Sample row 2 (first 12 fields): ['8150733', '27124', '44170767', '14917635', '0', '2025-12-05 02:30:11.630383+00', '0', '', '']
- Sample row 3 (first 12 fields): ['362171', '27124', '9166908', '12433492', '0', '2011-07-29 11:39:34.185736+00', '0', '', '']

### l_release_group_release_group

- Size bytes: 9422465
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['2', '8', '542668', '198743', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 2 (first 12 fields): ['4', '9', '698599', '142760', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']
- Sample row 3 (first 12 fields): ['5', '8', '680378', '532809', '0', '2011-05-16 15:03:23.368437+00', '0', '', '']

### l_release_group_series

- Size bytes: 11555571
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['31037', '166968', '1490318', '4041', '0', '2016-01-12 18:16:31.238462+00', '6', '', '']
- Sample row 2 (first 12 fields): ['29141', '166963', '1068154', '3525', '0', '2015-08-13 07:46:40.75198+00', '1', '', '']
- Sample row 3 (first 12 fields): ['29142', '254857', '1551908', '3064', '0', '2015-08-13 08:28:25.326954+00', '15', '', '']

### l_release_group_url

- Size bytes: 64953520
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['563783', '6309', '2143933', '5893089', '0', '2019-04-09 16:10:06.473456+00', '0', '', '']
- Sample row 2 (first 12 fields): ['497490', '6309', '1918019', '4990051', '0', '2018-04-30 23:00:28.28246+00', '0', '', '']
- Sample row 3 (first 12 fields): ['563784', '6314', '2143933', '5893090', '0', '2019-04-09 16:10:06.473456+00', '0', '', '']

### l_release_group_work

- Size bytes: 6966
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1', '1189071', '2436709', '22010', '0', '2025-03-06 23:13:01.253964+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2', '1189071', '40203', '12495624', '0', '2025-03-06 23:13:34.058936+00', '0', '', '']
- Sample row 3 (first 12 fields): ['3', '1191279', '42301', '14647462', '0', '2025-03-15 08:15:54.462154+00', '0', '', '']

### l_release_release

- Size bytes: 2018552
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['8288', '3', '406687', '406689', '0', '2015-07-16 23:18:28.014075+00', '0', '', '']
- Sample row 2 (first 12 fields): ['57828', '3', '72664', '628981', '0', '2015-07-16 23:22:13.500206+00', '0', '', '']
- Sample row 3 (first 12 fields): ['101594', '3', '428600', '1171377', '0', '2015-07-17 08:14:34.600393+00', '0', '', '']

### l_release_series

- Size bytes: 3864130
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['5755', '167218', '1700701', '73', '0', '2015-12-12 22:06:53.325787+00', '1', '', '']
- Sample row 2 (first 12 fields): ['3439', '167163', '3185453', '2653', '0', '2015-01-01 18:59:15.585023+00', '11', '', '']
- Sample row 3 (first 12 fields): ['4901', '167153', '1624184', '3575', '0', '2015-08-27 23:59:47.709916+00', '1', '', '']

### l_release_url

- Size bytes: 653760898
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['4731960', '6301', '3686654', '10270945', '0', '2022-12-07 09:38:57.325516+00', '0', '', '']
- Sample row 2 (first 12 fields): ['2461043', '6300', '2414175', '5980863', '0', '2019-05-13 18:55:55.160278+00', '0', '', '']
- Sample row 3 (first 12 fields): ['472293', '6300', '14', '45627', '0', '2011-05-16 16:31:52.155025+00', '0', '', '']

### l_series_series

- Size bytes: 372022
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['48', '207972', '3189', '3190', '0', '2015-05-07 15:15:32.621794+00', '0', '', '']
- Sample row 2 (first 12 fields): ['147', '207972', '4286', '4283', '0', '2016-03-20 12:11:20.92775+00', '0', '', '']
- Sample row 3 (first 12 fields): ['1144', '207972', '13102', '13103', '0', '2020-12-06 16:57:16.766576+00', '0', '', '']

### l_series_url

- Size bytes: 2054246
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['1876', '166980', '3265', '3122310', '0', '2015-05-28 12:11:17.853138+00', '0', '', '']
- Sample row 2 (first 12 fields): ['1877', '167006', '3265', '3122311', '0', '2015-05-28 12:11:17.853138+00', '0', '', '']
- Sample row 3 (first 12 fields): ['100', '166980', '182', '2518439', '0', '2014-05-15 15:38:06.164922+00', '0', '', '']

### l_series_work

- Size bytes: 3319500
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['16517', '290961', '4409', '12927125', '0', '2016-04-11 11:59:52.432684+00', '0', '', '']
- Sample row 2 (first 12 fields): ['14419', '167286', '3527', '492974', '0', '2015-08-14 07:46:09.272816+00', '1', '', '']
- Sample row 3 (first 12 fields): ['14422', '166981', '3527', '12628328', '0', '2015-08-14 07:46:09.272816+00', '2', '', '']

### l_url_work

- Size bytes: 27299014
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['260924', '27101', '8405234', '12653767', '0', '2021-08-06 06:34:14.666471+00', '0', '', '']
- Sample row 2 (first 12 fields): ['345748', '27101', '10327828', '14057280', '0', '2022-12-20 23:43:17.791088+00', '0', '', '']
- Sample row 3 (first 12 fields): ['260925', '27101', '8405235', '12653767', '0', '2021-08-06 06:34:14.666471+00', '0', '', '']

### l_work_work

- Size bytes: 32578040
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['13938', '27609', '12438362', '10900993', '0', '2026-05-26 18:00:47.140087+00', '3', '', '']
- Sample row 2 (first 12 fields): ['267930', '27609', '13401833', '13401836', '0', '2020-06-05 15:39:32.337794+00', '3', '', '']
- Sample row 3 (first 12 fields): ['195531', '27127', '12995321', '13110367', '0', '2017-05-15 22:15:47.900534+00', '0', '', '']

### label

- Size bytes: 43457287
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', 'f43e252d-9ebf-4e8e-bba8-36d080756cc1', 'Deleted Label', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['2', '39c4dc0c-badb-4ac3-b810-e4f374dff6d9', 'Certificate 18', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '2592', '4', '221']
- Sample row 3 (first 12 fields): ['103730', '6f70a5cb-99a7-4a42-9208-412446d4aa0f', 'Flo Master Inc.', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '4', '222']

### label_alias

- Size bytes: 3708796
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['6476', '68034', '4-Eyed Freak Remastering', '\\N', '0', '2012-06-06 09:50:00.044285+00', '1', '4-Eyed Freak Remastering', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['3553', '10383', 'KRAAK3', '\\N', '0', '2019-04-10 19:26:56.740969+00', '2', 'KRAAK3', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['6411', '27538', 'Halahup', '\\N', '0', '2012-05-16 21:08:08.309846+00', '\\N', 'Halahup', '\\N', '\\N', '\\N', '\\N']

### label_alias_type

- Size bytes: 117
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Label name', '\\N', '0', '\\N', '3a1a0c48-d885-3b89-87b2-9e8a483c5675']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '829662f2-a781-3ec8-8b46-fbcea6196f81']

### label_gid_redirect

- Size bytes: 640835
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['50b83275-dc81-45f9-b3ae-1ae43da39d1a', '366', '2011-05-16 14:57:06.530063+00']
- Sample row 2 (first 12 fields): ['feccf416-faa6-4c4a-9821-f9594ec5248c', '357', '2011-05-16 14:57:06.530063+00']
- Sample row 3 (first 12 fields): ['0ece1680-ab59-40c9-8e93-e658c39df65e', '568', '2011-05-16 14:57:06.530063+00']

### label_ipi

- Size bytes: 866616
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['3933', '00503527776', '0', '2012-10-10 05:00:08.24231+00']
- Sample row 2 (first 12 fields): ['31914', '00494059330', '0', '2012-05-15 19:04:48.684349+00']
- Sample row 3 (first 12 fields): ['3933', '00498735294', '0', '2012-10-10 05:00:08.24231+00']

### label_isni

- Size bytes: 26640
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['25513', '0000000106844624', '0', '2013-05-30 22:00:35.230397+00']
- Sample row 2 (first 12 fields): ['83683', '000000011781560X', '0', '2013-09-20 09:17:40.201068+00']
- Sample row 3 (first 12 fields): ['388', '0000000121076846', '0', '2015-02-11 06:03:48.498706+00']

### label_type

- Size bytes: 1187
- Sampled rows: 12
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Distributor', '\\N', '0', '\\N', '53ab8dcc-9946-3b62-966e-7634d78e5034']
- Sample row 2 (first 12 fields): ['2', 'Holding', '\\N', '0', '\\N', '43f31a62-97e4-36f6-9752-453c131b71ed']
- Sample row 3 (first 12 fields): ['3', 'Production', '\\N', '0', '\\N', 'a2426aab-2dd4-339c-b47d-b4923a241678']

### language

- Size bytes: 235960
- Sampled rows: 50
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['866', '\\N', '\\N', '\\N', 'Atikamekw', '1', 'atj']
- Sample row 2 (first 12 fields): ['4663', '\\N', '\\N', '\\N', 'Min Nan Chinese', '1', 'nan']
- Sample row 3 (first 12 fields): ['5690', '\\N', '\\N', '\\N', 'Réunion Creole French', '1', 'rcf']

### link

- Size bytes: 73082578
- Sampled rows: 50
- Field count (first row): 11
- Field count range (sampled): 11..11
- Distinct field counts (sampled): [11]
- Sample row 1 (first 12 fields): ['48067', '148', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '2', '2012-05-15 20:52:02.141676+00', 'f']
- Sample row 2 (first 12 fields): ['2', '6', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '0', '2011-05-16 15:03:23.368437+00', 'f']
- Sample row 3 (first 12 fields): ['3', '2', '\\N', '\\N', '\\N', '\\N', '\\N', '\\N', '0', '2011-05-16 15:03:23.368437+00', 'f']

### link_attribute

- Size bytes: 36772112
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['4', '477', '2011-05-16 15:03:23.368437+00']
- Sample row 2 (first 12 fields): ['6', '516', '2011-05-16 15:03:23.368437+00']
- Sample row 3 (first 12 fields): ['30847', '525', '2011-10-30 13:37:48.702986+00']

### link_attribute_credit

- Size bytes: 3275420
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['186492', '12', 'Chorus']
- Sample row 2 (first 12 fields): ['186493', '461', 'Gaya']
- Sample row 3 (first 12 fields): ['199487', '180', 'Yamaha and Steinway pianos']

### link_attribute_text_value

- Size bytes: 2117274
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['172527', '788', 'OCR00628']
- Sample row 2 (first 12 fields): ['172533', '788', 'OCR01657']
- Sample row 3 (first 12 fields): ['172535', '788', 'BuxWV 177']

### link_attribute_type

- Size bytes: 227694
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['1416', '\\N', '1416', '0', '24fb671b-b699-4887-a9ed-668a646088ca', 'winner', 'This attribute indicates the winning participant in a competition event.', '2024-06-04 13:08:29.500185+00']
- Sample row 2 (first 12 fields): ['835', '14', '14', '0', 'c1dbb66d-2356-417a-81ad-f688fee33257', 'guitarrón mexicano', 'The guitarrón mexicano is a very large and deep-bodied Mexican guitar-like instrument with six strings which is traditionally played in mariachi groups.', '2015-02-15 07:32:32.570132+00']
- Sample row 3 (first 12 fields): ['843', '14', '14', '0', '2474c241-d267-433a-a404-688b13c51d11', 'jouhikko', 'The jouhikko is a traditional, 2 or 3 stringed bowed lyre, from Finland and Karelia.', '2015-02-25 19:47:04.610414+00']

### link_creditable_attribute_type

- Size bytes: 4595
- Sampled rows: 50
- Field count (first row): 1
- Field count range (sampled): 1..1
- Distinct field counts (sampled): [1]
- Sample row 1 (first 12 fields): ['611']
- Sample row 2 (first 12 fields): ['613']
- Sample row 3 (first 12 fields): ['615']

### link_text_attribute_type

- Size bytes: 18
- Sampled rows: 4
- Field count (first row): 1
- Field count range (sampled): 1..1
- Distinct field counts (sampled): [1]
- Sample row 1 (first 12 fields): ['830']
- Sample row 2 (first 12 fields): ['788']
- Sample row 3 (first 12 fields): ['1080']

### link_type

- Size bytes: 208998
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['12', '\\N', '0', '38278b3b-30e6-304c-b0db-5ba701eb0268', 'release_group', 'release_group', 'covers and versions', '', 'covers or other versions', 'covers or other versions', 'covers and versions', '2014-05-18 09:46:23.72719+00']
- Sample row 2 (first 12 fields): ['735', '\\N', '0', '12678b88-1adb-3536-890e-9b39b9a14b2d', 'instrument', 'instrument', 'children', '', 'children', 'child of', 'has child', '2014-05-18 10:41:05.403719+00']
- Sample row 3 (first 12 fields): ['870', '784', '0', '4789521b-57b9-4689-9644-46de63190f66', 'series', 'url', 'soundcloud', 'This links a series (most commonly, but not necessarily always, a music festival) to its official page at SoundCloud.', 'SoundCloud', 'SoundCloud page for', 'has a SoundCloud page at', '2015-12-16 11:53:10.160133+00']

### link_type_attribute_type

- Size bytes: 22666
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['297', '1', '0', '1', '2023-12-13 20:51:21.816057+00']
- Sample row 2 (first 12 fields): ['966', '1', '0', '1', '2020-08-06 17:16:18.022868+00']
- Sample row 3 (first 12 fields): ['241', '511', '0', '1', '2015-05-28 17:38:11.221091+00']

### medium

- Size bytes: 574220821
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['3316155', '3046812', '1', '8', '', '0', '2026-02-02 15:00:52.332103+00', '8', '4ecb0d23-669b-33cd-911d-dd12c42dfb67']
- Sample row 2 (first 12 fields): ['5434234', '4997020', '1', '12', '', '0', '2025-05-19 18:02:17.904703+00', '1', '9560d0bb-b9e8-4050-8c1b-f4740304c056']
- Sample row 3 (first 12 fields): ['5500452', '5059003', '1', '12', '', '0', '2025-06-24 00:57:19.742813+00', '1', 'bb6dabf2-25aa-43b7-a8d1-226ba17d42ab']

### medium_cdtoc

- Size bytes: 75046757
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['1172670', '1065293', '972427', '0', '2022-12-21 00:01:43.25996+00']
- Sample row 2 (first 12 fields): ['1172671', '857624', '972428', '0', '2022-12-21 00:01:45.906277+00']
- Sample row 3 (first 12 fields): ['1172672', '337040', '972429', '0', '2022-12-21 00:01:48.363248+00']

### medium_format

- Size bytes: 12270
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['2', 'DVD', '\\N', '4', '1995', 'f', '\\N', '2875c583-4580-3a90-b723-ba1b39921e23']
- Sample row 2 (first 12 fields): ['3', 'SACD', '\\N', '5', '1999', 't', '\\N', '5c9021b5-aecf-3339-b78e-c7bbe427c342']
- Sample row 3 (first 12 fields): ['4', 'DualDisc', '\\N', '6', '2004', 't', '\\N', '564fb227-66af-356e-a4c6-f0a3f806fcd1']

### medium_gid_redirect

- Size bytes: 792360
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['781e8edd-ab79-3f45-8c31-11cbbb184f3a', '5058549', '2025-05-19 20:00:35.98181+00']
- Sample row 2 (first 12 fields): ['895c3559-0a9f-3616-a694-68e5c65f3252', '5422111', '2025-05-19 20:00:51.271326+00']
- Sample row 3 (first 12 fields): ['9ba42975-3bb9-387d-8950-2a25115e3dc3', '1820470', '2025-05-19 20:01:10.642106+00']

### orderable_link_type

- Size bytes: 86
- Sampled rows: 14
- Field count (first row): 2
- Field count range (sampled): 2..2
- Distinct field counts (sampled): [2]
- Sample row 1 (first 12 fields): ['232', '1']
- Sample row 2 (first 12 fields): ['1307', '2']
- Sample row 3 (first 12 fields): ['239', '1']

### place

- Size bytes: 12604747
- Sampled rows: 50
- Field count (first row): 17
- Field count range (sampled): 17..17
- Distinct field counts (sampled): [17]
- Sample row 1 (first 12 fields): ['11376', 'caa66bca-1a61-493c-90ef-342784f822c1', "Dunedin Muso's Club", '2', '12 Manse St, Dunedin, New Zealand 9016', '5430', '\\N', '', '0', '2015-05-19 03:41:51.076028+00', '1974', '\\N']
- Sample row 2 (first 12 fields): ['11377', 'd22f0929-169f-4053-a118-c5f29aa9aa1b', 'Joseph James Studios', '1', '', '68702', '\\N', '', '0', '2015-05-19 05:31:27.008206+00', '2007', '\\N']
- Sample row 3 (first 12 fields): ['1164', '4ed4324c-2210-45da-bf67-2118257dd925', 'Louis de Geer konsert & kongress', '2', 'Dalsgatan 15, 602 32 Norrköping, Sweden', '13049', '(58.588056,16.184444)', '', '0', '2013-11-09 20:44:31.532017+00', '1994', '\\N']

### place_alias

- Size bytes: 2135861
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['6360', '32256', 'Xinghai Concert Hall', 'en', '0', '2018-05-01 22:37:03.881902+00', '1', 'Xinghai Concert Hall', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['291', '1144', 'Finnlevy-studiot', '\\N', '0', '2013-10-19 03:43:42.174629+00', '\\N', 'Finnlevy-studiot', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['320', '1199', 'State Academic Mariinsky Theatre', 'en', '0', '2013-10-20 02:26:39.348351+00', '1', 'State Academic Mariinsky Theatre', '\\N', '\\N', '\\N', '\\N']

### place_alias_type

- Size bytes: 117
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Place name', '\\N', '0', '\\N', 'fb68f9a2-622c-319b-83b0-bbff4127cdc5']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '0a438b9c-1850-32de-b4bb-7f58f5048ea3']

### place_gid_redirect

- Size bytes: 106985
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['a07643c4-1bde-40c8-9778-6995b4a2f70f', '775', '2013-10-17 19:00:20.872511+00']
- Sample row 2 (first 12 fields): ['55f6d54b-b125-41e0-8ae5-fa56b5dc74f6', '784', '2013-10-18 03:00:26.524157+00']
- Sample row 3 (first 12 fields): ['ed98788e-8f53-43b7-b585-ccd24930d552', '914', '2013-10-18 06:00:25.174168+00']

### place_type

- Size bytes: 2167
- Sampled rows: 13
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['3', 'Other', '\\N', '99', '\\N', 'a0df5ead-0bd6-33d8-8444-855a9f3e9970']
- Sample row 2 (first 12 fields): ['1', 'Studio', '\\N', '1', 'A place designed for non-live production of music, typically a recording studio.', '05fa6a09-ff92-3d34-bdbb-5141d3c24f38']
- Sample row 3 (first 12 fields): ['2', 'Venue', '\\N', '2', 'A place that has live artistic performances as one of its primary functions, such as a concert hall.', 'cd92781a-a73f-30e8-a430-55d7521338db']

### recording

- Size bytes: 4451053943
- Sampled rows: 50
- Field count (first row): 9
- Field count range (sampled): 9..9
- Distinct field counts (sampled): [9]
- Sample row 1 (first 12 fields): ['20937085', '0f42ab32-22cd-4dcf-927b-a8d9a183d68b', 'Travelling Man', '2001233', '260000', '', '0', '2017-05-15 20:36:38.082509+00', 'f']
- Sample row 2 (first 12 fields): ['20937086', '4dce8f93-45ee-4573-8558-8cd321256233', 'Live Up', '2001233', '259000', '', '0', '2017-05-15 20:36:38.082509+00', 'f']
- Sample row 3 (first 12 fields): ['20937087', '48fabe3f-0fbd-4145-a917-83d164d6386f', 'Radiate', '2001233', '381000', '', '0', '2017-05-15 20:36:38.082509+00', 'f']

### recording_alias

- Size bytes: 144263969
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '7827570', 'Ungarische Rhapsodie Nr.15 A-Moll', '\\N', '0', '2015-05-18 21:05:02.544929+00', '\\N', 'Ungarische Rhapsodie Nr.15 A-Moll', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['2', '7827571', 'Ungarische Rhapsodie Nr.3 B-Dur', '\\N', '0', '2015-05-18 21:05:05.125509+00', '\\N', 'Ungarische Rhapsodie Nr.3 B-Dur', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['3', '7827572', 'Ungarische Rhapsodie Nr.2 Cis-Moll', '\\N', '0', '2015-05-18 21:05:13.205407+00', '\\N', 'Ungarische Rhapsodie Nr.2 Cis-Moll', '\\N', '\\N', '\\N', '\\N']

### recording_alias_type

- Size bytes: 121
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Recording name', '\\N', '0', '\\N', '5d564c8f-97de-3572-94bb-7f40ad661499']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', 'ba0dbaab-25c6-30a8-9da4-8568020ecdf3']

### recording_gid_redirect

- Size bytes: 371844742
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['2f020e89-6e85-4be9-9f04-519ec8ec8cfa', '39', '2011-05-16 16:08:20.288158+00']
- Sample row 2 (first 12 fields): ['6efd976a-109e-4146-9277-fb2af7d44910', '40', '2011-05-16 16:08:20.288158+00']
- Sample row 3 (first 12 fields): ['6a9774a6-d8df-4158-b35f-38265c1ea61f', '42', '2011-05-16 16:08:20.288158+00']

### release

- Size bytes: 756670216
- Sampled rows: 50
- Field count (first row): 14
- Field count range (sampled): 14..14
- Distinct field counts (sampled): [14]
- Sample row 1 (first 12 fields): ['9', '425cf29a-1490-43ab-abfa-7b17a2cec351', 'A Sorta Fairytale', '60', '896742', '1', '\\N', '120', '28', '\\N', '', '0']
- Sample row 2 (first 12 fields): ['10', 'a96e1d03-e685-3627-8cba-f5b96be7158f', 'A Sorta Fairytale', '60', '896742', '1', '\\N', '120', '28', '\\N', '', '0']
- Sample row 3 (first 12 fields): ['3257193', '6072a02d-e3cb-4f6a-b29c-526e8a0c4873', 'Kriminaltango et al', '1', '2823308', '\\N', '1', '\\N', '\\N', '\\N', '', '0']

### release_alias

- Size bytes: 7532451
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '1565845', 'Piano Sonatas, Volume I (disc 1)', '\\N', '0', '2015-05-18 21:05:26.603221+00', '\\N', 'Piano Sonatas, Volume I (disc 1)', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['2', '1604680', 'The Complete Songs of Robert Burns, Volume 1', '\\N', '0', '2015-05-18 21:05:42.057565+00', '\\N', 'The Complete Songs of Robert Burns, Volume 1', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['3', '1604680', 'The Complete Songs of Robert Burns, Volume 10', '\\N', '0', '2015-05-18 21:05:42.057565+00', '\\N', 'The Complete Songs of Robert Burns, Volume 10', '\\N', '\\N', '\\N', '\\N']

### release_alias_type

- Size bytes: 119
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Release name', '\\N', '0', '\\N', 'df187855-059b-3514-9d5e-d240de0b4228']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '02939c89-0e48-3357-bf41-bf8e4162c874']

### release_country

- Size bytes: 285139606
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['3', '81', '1997', '1', '24']
- Sample row 2 (first 12 fields): ['1427792', '107', '2014', '4', '23']
- Sample row 3 (first 12 fields): ['9', '81', '2002', '10', '14']

### release_gid_redirect

- Size bytes: 13958759
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['ea63f8e2-04ce-4591-a6ee-1137b9f2c51f', '91', '2011-05-16 15:59:00.785958+00']
- Sample row 2 (first 12 fields): ['2cd82e1c-9d36-4931-8f34-40c890c703aa', '100', '2011-05-16 15:59:00.785958+00']
- Sample row 3 (first 12 fields): ['306a8cb8-bfa6-46da-8984-c2b661e114bd', '133', '2011-05-16 15:59:00.785958+00']

### release_group

- Size bytes: 469828482
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['1964563', 'f59da930-70ba-4992-a346-7ed2d8e3cda8', 'Wande', '627364', '1', '', '0', '2018-04-30 23:56:50.245482+00']
- Sample row 2 (first 12 fields): ['2666236', '1cf5c673-171c-41fe-abfd-27a455013bbd', 'À nous', '2966520', '1', '', '0', '2021-04-22 19:12:59.273077+00']
- Sample row 3 (first 12 fields): ['13', '0eac6659-d590-3eb7-8c13-ed8b3fdf4ef7', 'The Inevitable', '11', '1', '', '0', '2009-05-24 20:47:00.490177+00']

### release_group_alias

- Size bytes: 8861406
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '138133', 'Earth Crossing', '\\N', '0', '2015-05-18 21:18:12.523239+00', '\\N', 'Earth Crossing', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['3', '1525479', 'Plate Records', '\\N', '0', '2015-05-19 02:02:24.259758+00', '\\N', 'Plate Records', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['4', '1525479', 'Plates Records', '\\N', '0', '2015-05-19 02:02:24.259758+00', '\\N', 'Plates Records', '\\N', '\\N', '\\N', '\\N']

### release_group_alias_type

- Size bytes: 125
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Release group name', '\\N', '0', '\\N', '156e24ca-8746-3cfc-99ae-0a867c765c67']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', 'abc2db8a-7386-354d-82f4-252c0213cbe4']

### release_group_gid_redirect

- Size bytes: 17020161
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['d2470fc4-0f73-318b-8e1f-d7dab4de10eb', '123848', '2011-05-16 14:57:06.530063+00']
- Sample row 2 (first 12 fields): ['cb00d099-e918-3f05-b4b3-67764fd49173', '123848', '2011-05-16 14:57:06.530063+00']
- Sample row 3 (first 12 fields): ['b9107578-1cb8-3628-be88-27b099d0ecf2', '123848', '2011-05-16 14:57:06.530063+00']

### release_group_primary_type

- Size bytes: 2913
- Sampled rows: 5
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Album', '\\N', '1', 'An album, perhaps better defined as a "Long Play" (LP) release, generally consists of previously unreleased material (unless this type is combined with secondary types which change that, such as "Compilation").', 'f529b476-6e62-324f-b0aa-1f3e33d313fc']
- Sample row 2 (first 12 fields): ['12', 'Broadcast', '\\N', '4', 'An episodic release that was originally broadcast via radio, television, or the Internet, including podcasts.', '3b2e49e1-2875-37b8-9fa9-1f7cf3f49900']
- Sample row 3 (first 12 fields): ['11', 'Other', '\\N', '99', "Any release that does not fit or can't decisively be placed in any of the other categories.", '4fc3be2b-de1e-396b-a933-beb8f1607a22']

### release_group_secondary_type

- Size bytes: 4051
- Sampled rows: 12
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['12', 'Field recording', '\\N', '0', 'A release mostly consisting of field recordings (such as nature sounds or city/industrial noise).', 'dbb7cf7d-47c6-42d1-a621-ab84849bc7b7']
- Sample row 2 (first 12 fields): ['11', 'Audio drama', '\\N', '0', 'An audio drama is an audio-only performance of a play (often, but not always, meant for radio). Unlike audiobooks, it usually has multiple performers rather than a main narrator.', '0eb547c2-8783-43e4-8f81-751c680e7b04']
- Sample row 3 (first 12 fields): ['5', 'Audiobook', '\\N', '0', 'An audiobook is a book read by a narrator without music.', '499a387e-6195-333e-91c0-9592bfec535e']

### release_group_secondary_type_join

- Size bytes: 36289598
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['2', '1', '2012-05-15 00:00:00+00']
- Sample row 2 (first 12 fields): ['661924', '1', '2022-12-20 23:40:47.252399+00']
- Sample row 3 (first 12 fields): ['24', '1', '2012-05-15 00:00:00+00']

### release_label

- Size bytes: 278650573
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['3262571', '3886289', '3267', '[none]', '2023-05-15 17:55:01.237161+00']
- Sample row 2 (first 12 fields): ['754743', '1219491', '72973', '\\N', '2012-11-13 13:25:10.730748+00']
- Sample row 3 (first 12 fields): ['5357', '5', '4934', '422-828 553-2', '2011-05-16 15:59:00.785958+00']

### release_packaging

- Size bytes: 2228
- Sampled rows: 24
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['16', 'Super Jewel Box', '\\N', '0', '\\N', 'dfb7da53-866f-4dfd-a016-80bafaeff3db']
- Sample row 2 (first 12 fields): ['3', 'Digipak', '\\N', '0', '\\N', '8f931351-d2e2-310f-afc6-37b89ddba246']
- Sample row 3 (first 12 fields): ['6', 'Keep Case', '\\N', '0', '\\N', 'bb14bb17-e8ad-375f-a3c6-b1f82fd2bcc4']

### release_status

- Size bytes: 1889
- Sampled rows: 7
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Official', '\\N', '1', 'Any release officially sanctioned by the artist and/or their record company. Most releases will fit into this category.', '4e304316-386d-3409-af2e-78857eec5cfe']
- Sample row 2 (first 12 fields): ['2', 'Promotion', '\\N', '2', 'A give-away release or a release intended to promote an upcoming official release (e.g. pre-release versions, releases included with a magazine, versions supplied to radio DJs for air-play).', '518ffc83-5cde-34df-8627-81bff5093d92']
- Sample row 3 (first 12 fields): ['3', 'Bootleg', '\\N', '3', 'An unofficial/underground release that was not sanctioned by the artist and/or the record company. This includes unofficial live recordings and pirated releases.', '1156806e-d06a-38bd-83f0-cf2284a808b9']

### release_unknown_country

- Size bytes: 8341475
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['1372866', '1998', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['4546586', '2004', '7', '20']
- Sample row 3 (first 12 fields): ['1147748', '2011', '12', '16']

### replication_control

- Size bytes: 42
- Sampled rows: 1
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['1', '31', '186857', '2026-06-20 00:20:53.569349+00']

### script

- Size bytes: 4055
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['92', 'Hani', '500', 'Han (Hanzi, Kanji, Hanja)', '4']
- Sample row 2 (first 12 fields): ['93', 'Hans', '501', 'Han (Simplified variant)', '4']
- Sample row 3 (first 12 fields): ['3', 'Ugar', '040', 'Ugaritic', '2']

### series

- Size bytes: 4112042
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['94', '476798b7-12ae-4237-9e2f-e2d4d6fab5f2', 'Panorama', 'Deutsche Grammophon', '2', '1', '0', '2014-05-14 22:51:40.00804+00']
- Sample row 2 (first 12 fields): ['170', '303ba27e-da80-4c3c-ae3e-3b3689c50174', 'Klassiset säveltäjät', '', '2', '1', '0', '2014-05-15 10:35:33.62127+00']
- Sample row 3 (first 12 fields): ['124', 'c172256d-ffc6-4b5d-acc8-334d7107e9e6', 'Techno & Dance', 'K-tel International Finland Oy', '1', '1', '0', '2014-05-15 02:00:38.687118+00']

### series_alias

- Size bytes: 515042
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['1', '12', 'Köchel catalogue', 'en', '0', '2014-05-14 18:28:59.030601+00', '1', 'Köchel catalogue', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['52', '260', 'OC ReMix', '\\N', '0', '2014-05-24 01:00:33.121714+00', '2', 'OC ReMix', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['4', '12', 'Köchelverzeichnis', 'de', '0', '2014-05-14 18:28:59.030601+00', '1', 'Köchelverzeichnis', '\\N', '\\N', '\\N', '\\N']

### series_alias_type

- Size bytes: 118
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Series name', '\\N', '0', '\\N', '0c615dbc-c7d6-39b3-b8da-bef465ce3046']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '8950366b-5ea3-32f2-bf74-ee482474c18b']

### series_gid_redirect

- Size bytes: 16211
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['77b29424-b17c-4449-aba9-57872e8ee7a5', '150', '2014-05-22 23:00:21.747736+00']
- Sample row 2 (first 12 fields): ['947ebd4f-2107-4ecf-a932-9e15315c1986', '1390', '2014-07-02 18:00:36.11706+00']
- Sample row 3 (first 12 fields): ['3c9e608d-49d3-4591-9eaa-dc91e39c06ed', '2041', '2014-08-28 13:00:19.04412+00']

### series_ordering_type

- Size bytes: 274
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Automatic', '\\N', '0', 'Sorts the items in the series automatically by their number attributes, using a natural sort order.', 'ec5fa247-c37a-39b3-b31d-bdac6207344e']
- Sample row 2 (first 12 fields): ['2', 'Manual', '\\N', '1', 'Allows for manually setting the position of each item in the series.', '2950ba43-3532-39e9-a7d1-7dc7e271fa25']

### series_type

- Size bytes: 2115
- Sampled rows: 19
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['7', 'Tour', 'event', '6', '0', 'A series of related concerts by an artist in different locations.', '8ff6df0e-3dce-3bdf-bd57-d386c51b0060']
- Sample row 2 (first 12 fields): ['8', 'Festival', 'event', '6', '1', 'A recurring festival, usually happening annually in the same location.', '44e9f6b1-34a7-3a17-a5d5-07bb100d8887']
- Sample row 3 (first 12 fields): ['9', 'Run', 'event', '6', '2', 'A series of performances of the same show at the same venue.', '975e9447-dfa8-3e29-82d6-07908fe36f3e']

### track

- Size bytes: 7636584934
- Sampled rows: 50
- Field count (first row): 12
- Field count range (sampled): 12..12
- Distinct field counts (sampled): [12]
- Sample row 1 (first 12 fields): ['34228823', '9b02977e-a03b-4a6b-a9a9-06e722bdcd7a', '428644', '3254461', '1', 'A1', 'The Ghost of Tom Joad', '813', '263000', '0', '2021-02-21 12:19:56.629604+00', 'f']
- Sample row 2 (first 12 fields): ['58102958', '1243819c-e129-4807-a70c-4ac36d5e633d', '208345', '6277244', '1', '1', 'Mine All Mine', '782', '315066', '0', '2026-05-31 05:30:34.952086+00', 'f']
- Sample row 3 (first 12 fields): ['35831997', '0b6b6283-a5a8-4560-9fa8-f68a430d86ea', '25849634', '3434937', '1', '1', 'Wonder Girl', '14389', '139000', '0', '2021-08-15 23:12:04.109954+00', 'f']

### track_gid_redirect

- Size bytes: 65943729
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['d8abbf14-5945-3639-a0c9-3e9c70b1c0a4', '11261036', '2014-10-15 00:00:09.772435+00']
- Sample row 2 (first 12 fields): ['446b27ef-92fd-3ea1-ad0a-fe1055196406', '11261031', '2014-10-15 00:00:09.772435+00']
- Sample row 3 (first 12 fields): ['403b9e19-a135-3acc-ae69-58fb0d735036', '11261025', '2014-10-15 00:00:09.772435+00']

### url

- Size bytes: 2469967056
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['5980861', 'd232c890-d7a6-4888-a65e-94744a334f45', 'https://open.spotify.com/artist/4QtHE540B0R3NREWskQJcA', '0', '2019-05-13 18:55:36.836953+00']
- Sample row 2 (first 12 fields): ['5980862', 'cfd6fdad-5bad-4cf3-81c0-bea569a5a7c5', 'https://www.deezer.com/artist/11438082', '0', '2019-05-13 18:55:36.836953+00']
- Sample row 3 (first 12 fields): ['3909279', 'df0d4cd1-0b14-4bfc-bb42-a27e3b41b7fc', 'https://rateyourmusic.com/artist/patrick_swayze', '0', '2016-10-12 02:36:03.302308+00']

### url_gid_redirect

- Size bytes: 2789384
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['67110028-cfd9-4007-b019-245729d911fb', '602751', '2011-05-16 16:31:52.155025+00']
- Sample row 2 (first 12 fields): ['423ea8ac-4be0-4b93-8cb7-1878db7ae888', '3944252', '2019-11-07 23:00:20.261302+00']
- Sample row 3 (first 12 fields): ['08c1eb7c-227c-46cf-8e0b-7d575cc1368c', '6408736', '2019-11-10 14:05:00.825052+00']

### work

- Size bytes: 285998409
- Sampled rows: 50
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['12431434', '86c6dd66-0da8-4b0f-8b78-ec9d4f12c5c4', 'Here to Go', '\\N', '', '0', '2011-07-02 15:29:31.688537+00']
- Sample row 2 (first 12 fields): ['12433103', '712ff9ea-20d2-4c1a-aab7-6e4b7df9b4b2', '1 John 4:16', '17', 'The Mountain Goats song', '0', '2011-09-09 02:36:50.327637+00']
- Sample row 3 (first 12 fields): ['12610029', 'ff2ebf6a-338d-468f-a0dd-ee5f489a962d', 'Goddess (of the Sad Man)', '17', '', '0', '2013-04-17 11:06:21.906341+00']

### work_alias

- Size bytes: 22771901
- Sampled rows: 50
- Field count (first row): 16
- Field count range (sampled): 16..16
- Distinct field counts (sampled): [16]
- Sample row 1 (first 12 fields): ['53093', '3688857', 'Kaihola (edit)', '\\N', '0', '2012-10-05 23:00:22.399026+00', '\\N', 'Kaihola (edit)', '\\N', '\\N', '\\N', '\\N']
- Sample row 2 (first 12 fields): ['47458', '12495572', 'Gonna Send You Back to Georgia', '\\N', '0', '2012-05-15 20:40:26.06643+00', '\\N', 'Gonna Send You Back to Georgia', '\\N', '\\N', '\\N', '\\N']
- Sample row 3 (first 12 fields): ['47461', '7430219', "I'm Cryin'", '\\N', '0', '2012-05-15 22:26:27.988007+00', '\\N', "I'm Cryin'", '\\N', '\\N', '\\N', '\\N']

### work_alias_type

- Size bytes: 116
- Sampled rows: 2
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['1', 'Work name', '\\N', '0', '\\N', 'a18cab3f-0ae2-3978-8f75-dd9c09702b25']
- Sample row 2 (first 12 fields): ['2', 'Search hint', '\\N', '0', '\\N', '02238bc1-dfd8-39a8-bbf8-c697747291ec']

### work_attribute

- Size bytes: 73121758
- Sampled rows: 50
- Field count (first row): 5
- Field count range (sampled): 5..5
- Distinct field counts (sampled): [5]
- Sample row 1 (first 12 fields): ['1', '12108628', '3', '\\N', '175-8691-7']
- Sample row 2 (first 12 fields): ['2', '7565110', '1', '22', '\\N']
- Sample row 3 (first 12 fields): ['4', '12429749', '1', '14', '\\N']

### work_attribute_type

- Size bytes: 8394
- Sampled rows: 50
- Field count (first row): 8
- Field count range (sampled): 8..8
- Distinct field counts (sampled): [8]
- Sample row 1 (first 12 fields): ['1', 'Key', '', 'f', '\\N', '0', '\\N', '7526c19d-3be4-3420-b6cc-9fb6e49fa1a9']
- Sample row 2 (first 12 fields): ['14', 'Identifiers', '', 'f', '\\N', '1', '\\N', '588e1c46-c825-3aea-9068-9907f5c7c736']
- Sample row 3 (first 12 fields): ['4', 'Rāga (Carnatic)', '', 'f', '\\N', '2', '\\N', 'fc6b57cc-d017-3a60-a59a-cbd23854b9d4']

### work_attribute_type_allowed_value

- Size bytes: 68066
- Sampled rows: 50
- Field count (first row): 7
- Field count range (sampled): 7..7
- Distinct field counts (sampled): [7]
- Sample row 1 (first 12 fields): ['1051', '1', 'C Phrygian', '\\N', '57', '\\N', '8c867b48-04b9-334a-8d58-ddbab115ca33']
- Sample row 2 (first 12 fields): ['1052', '1', 'D Phrygian', '\\N', '58', '\\N', '0229b015-27f7-3a1d-838b-e5a49676b7be']
- Sample row 3 (first 12 fields): ['1053', '1', 'E Phrygian', '\\N', '59', '\\N', 'e9baec8f-a68e-3eb9-a5dc-c1f1730797cd']

### work_gid_redirect

- Size bytes: 11916700
- Sampled rows: 50
- Field count (first row): 3
- Field count range (sampled): 3..3
- Distinct field counts (sampled): [3]
- Sample row 1 (first 12 fields): ['fe8ee557-7567-3cec-b9cf-f046988193ed', '7827807', '2011-05-16 16:27:39.450042+00']
- Sample row 2 (first 12 fields): ['6be22572-d027-3c3f-be17-57224abd6590', '492584', '2011-05-16 16:27:39.450042+00']
- Sample row 3 (first 12 fields): ['515d604a-8bd4-3d59-8dee-c57152966962', '78885', '2011-05-16 16:27:39.450042+00']

### work_language

- Size bytes: 80688369
- Sampled rows: 50
- Field count (first row): 4
- Field count range (sampled): 4..4
- Distinct field counts (sampled): [4]
- Sample row 1 (first 12 fields): ['12610029', '120', '0', '2017-05-15 19:14:58.003429+00']
- Sample row 2 (first 12 fields): ['9709654', '198', '0', '2017-05-15 19:14:58.003429+00']
- Sample row 3 (first 12 fields): ['12610030', '120', '0', '2017-05-15 19:14:58.003429+00']

### work_type

- Size bytes: 5984
- Sampled rows: 29
- Field count (first row): 6
- Field count range (sampled): 6..6
- Distinct field counts (sampled): [6]
- Sample row 1 (first 12 fields): ['29', 'Musical', '\\N', '2', 'Musical theatre is a form of theatrical performance that combines songs, spoken dialogue, acting, and dance.', '9ca5e067-acf7-3cd6-baa4-92bf1975bf24']
- Sample row 2 (first 12 fields): ['1', 'Aria', '\\N', '2', 'An aria is a self-contained piece for one voice usually with orchestral accompaniment. They are most common inside operas, but also appear in cantatas, oratorios and even on their own (concert arias).', 'ae801f48-7a7f-3af6-91c7-456f82dae8a9']
- Sample row 3 (first 12 fields): ['2', 'Ballet', '\\N', '2', 'A ballet is music composed to be used, together with a choreography, for a ballet dance production.', '6a90744c-1e07-3b88-b394-cd44cd68bd63']

