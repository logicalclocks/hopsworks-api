# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                             |    Stmts |     Miss |   Cover |   Missing |
|--------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| python/hopsworks/\_\_init\_\_.py                                                 |      259 |       84 |     68% |85-87, 91-93, 103-105, 157, 160, 163, 172-174, 285, 313, 317, 325, 350, 376-378, 405-410, 424, 454, 460, 470, 472-504, 518, 551-553, 603-620, 631-633, 658-660, 683-685, 696-698, 716, 726 |
| python/hopsworks/alert/\_\_init\_\_.py                                           |        6 |        6 |      0% |      5-10 |
| python/hopsworks/alert\_receiver/\_\_init\_\_.py                                 |        6 |        6 |      0% |      5-10 |
| python/hopsworks/app/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/cli/\_\_init\_\_.py                                             |        0 |        0 |    100% |           |
| python/hopsworks/cli/\_\_main\_\_.py                                             |        3 |        3 |      0% |       3-7 |
| python/hopsworks/cli/auth.py                                                     |       62 |        2 |     97% |   41, 107 |
| python/hopsworks/cli/commands/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks/cli/commands/admin.py                                           |       72 |        0 |    100% |           |
| python/hopsworks/cli/commands/agent.py                                           |      208 |       41 |     80% |40-41, 72-73, 179-180, 206-207, 232-233, 268-269, 274-275, 278-279, 379-380, 382-383, 385-386, 392-404, 414-415, 417-418, 438, 441-442, 470-471, 522, 541 |
| python/hopsworks/cli/commands/alert.py                                           |      325 |      120 |     63% |68-69, 107-108, 125-126, 129-135, 175-176, 178-179, 202-203, 228-250, 297-312, 340-341, 344-345, 347-348, 363-370, 436-437, 454-455, 457-458, 483-484, 502-509, 537-538, 540-541, 567-575, 607-608, 612-613, 639-647, 675-688, 701-702, 704, 712-719, 724-730, 739, 743, 745 |
| python/hopsworks/cli/commands/analytics.py                                       |      428 |       45 |     89% |168, 189, 201, 238, 292-293, 346, 404, 408, 410, 413, 416, 423, 426, 436, 443, 497, 501, 578-579, 596-597, 767, 785-786, 792-793, 858, 863-864, 880, 909-911, 951-962 |
| python/hopsworks/cli/commands/app.py                                             |      324 |       32 |     90% |46-47, 134, 177, 182-183, 235-236, 425, 435, 442, 444, 471, 496-497, 561-562, 592-593, 607-608, 634, 641-642, 659-660, 685, 696, 703-704, 708, 723 |
| python/hopsworks/cli/commands/build.py                                           |      486 |      149 |     69% |66-80, 102-110, 114-115, 117, 168, 177, 182-183, 226, 307, 428, 507, 519-536, 540-565, 569-584, 588-603, 613, 619-667, 672, 674, 677-700, 705, 707-719, 722-739, 744-756, 766, 793-799, 807, 853, 963-964, 976 |
| python/hopsworks/cli/commands/context.py                                         |       88 |       24 |     73% |112, 124-131, 134-137, 140-145, 148-155, 187, 231-232 |
| python/hopsworks/cli/commands/datasource.py                                      |      874 |      131 |     85% |87-88, 92-95, 107-122, 126, 181, 317, 319, 321, 375, 484, 630, 1735, 1770-1771, 1774, 1796-1797, 1809-1810, 1821-1822, 1829-1830, 1880-1882, 1885, 1915-1916, 1933-1934, 1936-1937, 1960-1961, 1982, 1993-1994, 2012-2013, 2015-2016, 2106, 2120-2121, 2123-2124, 2147-2152, 2154-2155, 2169-2192, 2210-2224, 2237-2247, 2278-2279, 2292-2293, 2296-2297, 2309, 2324-2325, 2327, 2336-2340, 2375-2376, 2383-2386, 2498, 2635-2636, 2655-2656, 2682, 2917-2918, 2941-2942 |
| python/hopsworks/cli/commands/deployment.py                                      |      299 |      106 |     65% |37-38, 69-70, 74-90, 119-160, 164-167, 171-179, 192-197, 217, 242, 358-359, 361, 367-382, 396-397, 429, 463-464, 489-490, 530, 536-537, 542-543, 546-547, 656-657, 659-660, 666-682, 692-693, 695-696, 716, 719-720, 762-763, 765 |
| python/hopsworks/cli/commands/env.py                                             |       72 |       21 |     71% |32, 48-52, 95-115, 172-173, 207-208, 210 |
| python/hopsworks/cli/commands/factory.py                                         |      489 |       82 |     83% |89-95, 124-131, 156-163, 210, 217, 262, 275, 293, 296, 300-304, 312, 328, 408, 415, 421, 438, 457-458, 468-469, 471, 492-493, 513, 540, 555, 567, 590-591, 594, 596, 600, 609, 611-612, 651-683, 695, 699-701, 732-733, 795, 832-833, 947-948 |
| python/hopsworks/cli/commands/fg.py                                              |      557 |      149 |     73% |146-147, 149-150, 228-229, 344-346, 506-538, 647, 652, 657-658, 669, 671, 755-764, 770-771, 775, 777, 820-863, 898, 901-902, 945, 947, 967-968, 994-995, 999-1025, 1069-1070, 1073-1076, 1108-1109, 1112-1115, 1154-1155, 1181-1182, 1201-1202, 1227-1228, 1251-1252, 1261-1267, 1282-1283, 1299, 1305, 1325, 1349-1369, 1391, 1400-1408, 1412-1415 |
| python/hopsworks/cli/commands/files.py                                           |      103 |       31 |     70% |43-44, 77-78, 115-116, 141-148, 181-190, 214-221, 237, 241-242 |
| python/hopsworks/cli/commands/fv.py                                              |      326 |      128 |     61% |61-62, 64, 98-108, 178-194, 290-291, 293, 299-300, 303-304, 308, 330-331, 392-421, 453-454, 459, 497-519, 549, 555-556, 572-577, 598-603, 623-627, 642-654, 673-678, 700-704, 713-717, 727, 738, 747-767, 771-779 |
| python/hopsworks/cli/commands/git.py                                             |       81 |       10 |     88% |141-142, 144-147, 203-212, 225-228, 251-252 |
| python/hopsworks/cli/commands/init.py                                            |        8 |        0 |    100% |           |
| python/hopsworks/cli/commands/job.py                                             |      389 |      164 |     58% |32, 35-42, 52, 54-58, 78-79, 108-109, 113-126, 130-143, 202-203, 210, 213-214, 287-339, 391, 400-401, 410, 459-480, 493-501, 566, 569-572, 579-580, 584-585, 594-595, 598-599, 763-764, 767, 780-794, 810-811, 827-834, 847-852, 869-874, 889-894, 909-915, 921, 925, 933-934, 936, 943-944, 960-963 |
| python/hopsworks/cli/commands/login.py                                           |       27 |       17 |     37% |     53-84 |
| python/hopsworks/cli/commands/logout.py                                          |       18 |        1 |     94% |        33 |
| python/hopsworks/cli/commands/mlsystem.py                                        |      335 |       62 |     81% |57-58, 66, 122, 145-146, 151, 195-196, 219, 248-249, 344, 349, 364-366, 389, 391, 405, 472-497, 519, 569-570, 587-597, 636-637, 656, 676, 728, 744 |
| python/hopsworks/cli/commands/model.py                                           |      160 |       51 |     68% |64-65, 68, 87-103, 108, 204, 209-214, 218-226, 239-240, 273-274, 292-299, 340, 342-345, 350, 355, 359-360 |
| python/hopsworks/cli/commands/project.py                                         |       64 |        6 |     91% |63-64, 107-118 |
| python/hopsworks/cli/commands/search.py                                          |       48 |        4 |     92% |110, 128-129, 187 |
| python/hopsworks/cli/commands/session.py                                         |      753 |      294 |     61% |51-53, 169, 247-248, 293-294, 331-332, 363-367, 374, 426, 456-460, 503, 599, 615-621, 804-805, 833, 845, 880-893, 910, 951-996, 1052, 1063-1070, 1074, 1080, 1116, 1127-1128, 1146-1153, 1162-1190, 1197, 1201, 1220, 1258, 1262, 1273-1286, 1289-1295, 1308-1330, 1354-1397, 1411-1437, 1467-1473, 1502-1503, 1526-1534, 1567-1570, 1580-1589, 1599-1609, 1619-1636, 1654-1720, 1736-1781, 1806-1830 |
| python/hopsworks/cli/commands/setup.py                                           |      248 |       50 |     80% |77-84, 105, 113-114, 137, 175-193, 228, 368-371, 385, 389-390, 403, 449, 479-482, 511-512, 598-600, 605, 607, 609, 629, 632 |
| python/hopsworks/cli/commands/skills.py                                          |      170 |       25 |     85% |48, 81-82, 84, 168-169, 173, 193-215, 295, 302, 401-402, 406 |
| python/hopsworks/cli/commands/superset.py                                        |      141 |       38 |     73% |51-52, 91-101, 116-122, 141-142, 155, 199-200, 217, 220-221, 255, 278-279, 295-301, 337-339, 345-346, 354 |
| python/hopsworks/cli/commands/td.py                                              |      198 |       81 |     59% |53-54, 58, 62-63, 137-138, 146, 189-217, 244-258, 337-349, 374-379, 411-416, 427-430, 432, 444, 447-449, 460, 464-465, 472-480 |
| python/hopsworks/cli/commands/transformation.py                                  |       84 |       13 |     85% |36-37, 98, 103-104, 115-116, 173, 180-181, 184, 191, 194 |
| python/hopsworks/cli/commands/trino.py                                           |      134 |       79 |     41% |66-67, 72-73, 78-87, 92-97, 110-112, 116-118, 129-156, 222-233, 253, 266, 281-287, 302-307, 321-332 |
| python/hopsworks/cli/commands/update.py                                          |       49 |       15 |     69% |54, 69-70, 73, 76, 87-96, 107-108 |
| python/hopsworks/cli/config.py                                                   |      155 |       22 |     86% |75, 89, 105, 109-110, 119-121, 140, 144, 149-151, 168-169, 245, 247, 331-337 |
| python/hopsworks/cli/factory\_spec.py                                            |      434 |       78 |     82% |94, 133, 142, 144, 209-210, 217-218, 221, 225, 227, 248-249, 266-267, 274, 276, 280, 303, 305-310, 319, 324, 326, 342-343, 349, 355, 360, 365-366, 369, 375, 377, 385, 393-394, 408-426, 437-438, 443-444, 447, 451, 453, 637, 645, 651, 653, 658, 660, 665, 668, 704, 789, 853-855 |
| python/hopsworks/cli/git\_sync.py                                                |      375 |      140 |     63% |73-74, 139-163, 172-197, 224-225, 231-232, 258-259, 264-287, 336, 358-360, 367-382, 387-397, 402-412, 421-480, 505, 522-524, 538-545, 548-550, 575-576, 581-583, 612, 663-664, 666, 684, 697, 701-702, 709-715, 722, 740 |
| python/hopsworks/cli/health.py                                                   |      147 |       23 |     84% |34, 38-39, 41, 46, 50, 56, 60-62, 102-103, 115-120, 191-192, 203-204, 292, 311-312 |
| python/hopsworks/cli/joinspec.py                                                 |       18 |        0 |    100% |           |
| python/hopsworks/cli/lineage.py                                                  |       33 |        4 |     88% | 55, 62-64 |
| python/hopsworks/cli/main.py                                                     |      114 |        8 |     93% |89, 105, 124-125, 345-347, 359 |
| python/hopsworks/cli/output.py                                                   |       85 |        7 |     92% |73, 112-113, 124-133, 247, 258 |
| python/hopsworks/cli/pipeline\_data.py                                           |      204 |       15 |     93% |57, 65, 108, 117, 145, 282-283, 296-298, 308, 312, 325, 329, 346 |
| python/hopsworks/cli/redact.py                                                   |       39 |        1 |     97% |        54 |
| python/hopsworks/cli/scaffold.py                                                 |      121 |        4 |     97% |177, 205, 208, 316 |
| python/hopsworks/cli/session.py                                                  |       49 |        8 |     84% |70, 82-83, 99, 101-103, 121 |
| python/hopsworks/cli/silver\_status.py                                           |      115 |       12 |     90% |65, 70, 90-91, 100, 114, 116-117, 135, 152, 179-180 |
| python/hopsworks/cli/system\_doc.py                                              |      237 |       15 |     94% |107, 115, 173, 291-292, 336-337, 394, 397-398, 404, 411, 437-439 |
| python/hopsworks/cli/teardown.py                                                 |      320 |       69 |     78% |161-163, 337-338, 387, 396-397, 402, 419, 424-431, 438-442, 446-462, 465-469, 472-476, 501-543, 606, 610-612, 636, 639, 646, 648, 653, 667 |
| python/hopsworks/cli/templates/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/cli/terminal\_api.py                                            |       18 |        3 |     83% |     93-97 |
| python/hopsworks/cli/versions.py                                                 |       23 |        0 |    100% |           |
| python/hopsworks/client/\_\_init\_\_.py                                          |       15 |        0 |    100% |           |
| python/hopsworks/client/auth/\_\_init\_\_.py                                     |        4 |        4 |      0% |       5-8 |
| python/hopsworks/client/base/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/exceptions/\_\_init\_\_.py                               |       21 |        0 |    100% |           |
| python/hopsworks/client/external/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/hopsworks/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/command/\_\_init\_\_.py                                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/connection/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/constants.py                                                    |        2 |        2 |      0% |     17-44 |
| python/hopsworks/core/\_\_init\_\_.py                                            |       10 |       10 |      0% |      5-14 |
| python/hopsworks/core/alerts\_api/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/app\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/costs\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/dataset\_api/\_\_init\_\_.py                               |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/env\_var\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/environment\_api/\_\_init\_\_.py                           |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/execution\_api/\_\_init\_\_.py                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/git\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/git\_op\_execution\_api/\_\_init\_\_.py                    |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/git\_provider\_api/\_\_init\_\_.py                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/git\_remote\_api/\_\_init\_\_.py                           |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/hosts\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/ingestion\_job/\_\_init\_\_.py                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/ingestion\_job\_conf/\_\_init\_\_.py                       |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/inode/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/job\_api/\_\_init\_\_.py                                   |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/job\_configuration/\_\_init\_\_.py                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/kafka\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/keywords\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/library\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/opensearch/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/opensearch\_api/\_\_init\_\_.py                            |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/project\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/project\_members\_api/\_\_init\_\_.py                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/rest\_endpoint/\_\_init\_\_.py                             |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/search\_api/\_\_init\_\_.py                                |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/secret\_api/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/services\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/superset\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tag\_schemas\_api/\_\_init\_\_.py                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tags\_api/\_\_init\_\_.py                                  |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/trino\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/trino\_catalog\_api/\_\_init\_\_.py                        |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/users\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/variable\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/cost/\_\_init\_\_.py                                            |        5 |        5 |      0% |       5-9 |
| python/hopsworks/decorators/\_\_init\_\_.py                                      |        8 |        8 |      0% |      5-12 |
| python/hopsworks/engine/\_\_init\_\_.py                                          |        0 |        0 |    100% |           |
| python/hopsworks/engine/environment\_engine/\_\_init\_\_.py                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/engine/execution\_engine/\_\_init\_\_.py                        |        2 |        2 |      0% |       5-6 |
| python/hopsworks/engine/git\_engine/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/env\_var/\_\_init\_\_.py                                        |        2 |        2 |      0% |       5-6 |
| python/hopsworks/environment/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/execution/\_\_init\_\_.py                                       |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_commit/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_file\_status/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_op\_execution/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_provider/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_remote/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/git\_repo/\_\_init\_\_.py                                       |        2 |        2 |      0% |       5-6 |
| python/hopsworks/gpu\_availability/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/job/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/job\_schedule/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/kafka\_schema/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/kafka\_topic/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hopsworks/library/\_\_init\_\_.py                                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/mcp/\_\_init\_\_.py                                             |        3 |        0 |    100% |           |
| python/hopsworks/mcp/models/\_\_init\_\_.py                                      |        0 |        0 |    100% |           |
| python/hopsworks/mcp/models/dataset.py                                           |       25 |        0 |    100% |           |
| python/hopsworks/mcp/models/feature\_group.py                                    |       20 |        0 |    100% |           |
| python/hopsworks/mcp/models/job.py                                               |       34 |        1 |     97% |        75 |
| python/hopsworks/mcp/models/project.py                                           |        3 |        0 |    100% |           |
| python/hopsworks/mcp/prompts/\_\_init\_\_.py                                     |        2 |        0 |    100% |           |
| python/hopsworks/mcp/prompts/project.py                                          |       26 |       11 |     58% |48-51, 62-65, 76-79, 87, 95 |
| python/hopsworks/mcp/prompts/system.py                                           |       12 |        2 |     83% |    43, 51 |
| python/hopsworks/mcp/resources/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/mcp/resources/project.py                                        |       28 |       14 |     50% |59-64, 86-90, 108-113 |
| python/hopsworks/mcp/run\_server.py                                              |       69 |       29 |     58% |41-45, 53-55, 174, 179, 193-234 |
| python/hopsworks/mcp/server.py                                                   |       31 |        2 |     94% |   69, 107 |
| python/hopsworks/mcp/tools/\_\_init\_\_.py                                       |        6 |        0 |    100% |           |
| python/hopsworks/mcp/tools/auth.py                                               |       15 |        4 |     73% |     78-95 |
| python/hopsworks/mcp/tools/dataset.py                                            |       72 |       49 |     32% |75-89, 122-137, 170-184, 222-239, 267-273, 288-297 |
| python/hopsworks/mcp/tools/feature\_group.py                                     |       67 |       45 |     33% |60-67, 72-81, 87-95, 108-111, 120-125, 150-179, 190-195 |
| python/hopsworks/mcp/tools/jobs.py                                               |       25 |       11 |     56% |59-66, 81-89 |
| python/hopsworks/mcp/tools/project.py                                            |       58 |       36 |     38% |63-64, 76-90, 114-134, 151-157, 179-183, 203-208 |
| python/hopsworks/mcp/tools/terminal.py                                           |       59 |       36 |     39% |36-38, 62-65, 81-111, 123-127, 141-147, 162-168 |
| python/hopsworks/mcp/utils/\_\_init\_\_.py                                       |        0 |        0 |    100% |           |
| python/hopsworks/mcp/utils/auth.py                                               |       11 |        6 |     45% |     57-76 |
| python/hopsworks/mcp/utils/tags.py                                               |       15 |        0 |    100% |           |
| python/hopsworks/project/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hopsworks/project\_member/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/secret/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/spark.py                                                        |       17 |       17 |      0% |     18-98 |
| python/hopsworks/tag/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/triggered\_alert/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/user/\_\_init\_\_.py                                            |        3 |        3 |      0% |       5-7 |
| python/hopsworks/util/\_\_init\_\_.py                                            |       31 |       31 |      0% |      5-35 |
| python/hopsworks/version.py                                                      |        2 |        2 |      0% |     17-22 |
| python/hopsworks\_common/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hopsworks\_common/alert.py                                                |      160 |       48 |     70% |97-109, 123, 135, 141, 147, 153, 156, 164, 173, 176, 196-207, 213, 219, 225, 229, 241, 260-270, 276, 282, 286, 297, 317-328, 334, 340, 346, 350, 362, 383-395, 401, 407, 413, 419, 423, 436 |
| python/hopsworks\_common/alert\_receiver.py                                      |      212 |       94 |     56% |32-37, 40, 48, 69, 74, 77, 81, 87, 90, 100-105, 108, 116, 137, 142, 145, 149, 155, 158, 169-177, 180, 184, 199-201, 205-208, 213, 218, 221, 225, 232, 235, 245-250, 253, 257, 278, 283, 286, 290, 296, 299, 346-347, 359, 365, 371, 377, 380, 384-399, 402, 405-413, 416 |
| python/hopsworks\_common/alert\_route.py                                         |       50 |       12 |     76% |50-51, 56, 61, 66, 71, 76, 86, 89, 93, 104, 107 |
| python/hopsworks\_common/app.py                                                  |      385 |       20 |     95% |155, 164, 207, 265, 281, 293, 299, 305, 386, 403, 452, 458, 647-650, 694-695, 766, 772 |
| python/hopsworks\_common/client/\_\_init\_\_.py                                  |       87 |       14 |     84% |45-61, 82, 95, 128, 136, 146, 186, 194 |
| python/hopsworks\_common/client/auth.py                                          |       36 |       12 |     67% |39-40, 52, 55-56, 77-83 |
| python/hopsworks\_common/client/base.py                                          |      283 |       49 |     83% |78-83, 91-96, 100, 104, 108-109, 120, 123-124, 167, 214, 221, 225, 244, 262-263, 277, 304, 433, 438, 444, 465, 510, 520-522, 530-534, 538-543, 547-554 |
| python/hopsworks\_common/client/exceptions.py                                    |      153 |        8 |     95% |52-54, 60, 160-164, 172, 184 |
| python/hopsworks\_common/client/external.py                                      |      206 |       74 |     64% |63-106, 121, 127-139, 144-189, 192-196, 271-294, 392-393, 401-405, 432, 435, 439, 443 |
| python/hopsworks\_common/client/hopsworks.py                                     |      102 |       63 |     38% |56-82, 86-92, 96, 99, 102, 105, 113-118, 126-131, 134-142, 145-149, 157-164, 175, 178, 182 |
| python/hopsworks\_common/client/istio/\_\_init\_\_.py                            |       14 |        5 |     64% |     29-34 |
| python/hopsworks\_common/client/istio/base.py                                    |       30 |       13 |     57% |52-57, 65-70, 74, 77 |
| python/hopsworks\_common/client/istio/external.py                                |       27 |       13 |     52% |44-55, 59, 70, 73, 77 |
| python/hopsworks\_common/client/istio/grpc/\_\_init\_\_.py                       |        0 |        0 |    100% |           |
| python/hopsworks\_common/client/istio/grpc/errors.py                             |        7 |        2 |     71% |    30, 33 |
| python/hopsworks\_common/client/istio/grpc/exceptions.py                         |       58 |       58 |      0% |    19-131 |
| python/hopsworks\_common/client/istio/grpc/inference\_client.py                  |       48 |       29 |     40% |29, 32-41, 53-58, 69-84, 87, 90, 94, 98, 101-117 |
| python/hopsworks\_common/client/istio/grpc/proto/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| python/hopsworks\_common/client/istio/grpc/proto/grpc\_predict\_v2\_pb2.py       |      148 |       67 |     55% |   382-450 |
| python/hopsworks\_common/client/istio/grpc/proto/grpc\_predict\_v2\_pb2\_grpc.py |       76 |       43 |     43% |39-74, 91-93, 102-104, 113-115, 127-129, 141-143, 155-157, 166-168, 177-179, 183-228, 248, 277, 306, 335, 364, 393, 422, 451 |
| python/hopsworks\_common/client/istio/hopsworks.py                               |       60 |       29 |     52% |52-64, 67-76, 79-83, 86, 90-92, 103, 106 |
| python/hopsworks\_common/client/istio/utils/\_\_init\_\_.py                      |        0 |        0 |    100% |           |
| python/hopsworks\_common/client/istio/utils/infer\_type.py                       |      330 |      233 |     29% |55, 75-105, 121-123, 126-129, 140, 151, 159, 222, 231, 234-241, 259-316, 320-336, 374-375, 379-389, 404-417, 425-452, 465-474, 500-507, 518, 529, 534, 545, 556, 564, 572-579, 595-652, 683-692, 696-706, 717-727, 740-755, 763-790 |
| python/hopsworks\_common/client/istio/utils/numpy\_codec.py                      |       35 |       29 |     17% |24-39, 44-70 |
| python/hopsworks\_common/client/online\_store\_rest\_client.py                   |      338 |       65 |     81% |73, 92-98, 109-113, 115, 151, 155, 180-187, 202, 204, 208, 211, 217, 225, 244, 276, 285, 301, 306-309, 331, 339, 344, 349, 355, 418, 481-483, 500, 560-566, 580, 606, 613, 620, 629, 635, 645, 650-651, 662-675, 701, 706, 714 |
| python/hopsworks\_common/command.py                                              |       25 |        4 |     84% |     49-52 |
| python/hopsworks\_common/connection.py                                           |      292 |       96 |     67% |170-174, 182-186, 194-198, 221-223, 268, 303, 318-329, 339, 352, 366, 372-377, 401-469, 552, 556, 560-562, 651, 665, 670, 674, 679, 683, 688, 692, 697, 701, 706, 715, 719, 723, 728, 737-740, 745, 750, 753-754, 757 |
| python/hopsworks\_common/constants.py                                            |      207 |        2 |     99% |    36, 39 |
| python/hopsworks\_common/core/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks\_common/core/alerts\_api.py                                     |      270 |      170 |     37% |178-181, 213-216, 245-248, 281-291, 325-336, 375-387, 423-436, 477-491, 540-574, 624-662, 712-737, 778-802, 830-834, 868-873, 916-981, 1005-1009, 1052-1084, 1117-1121, 1133-1136, 1144-1189, 1201-1205, 1208-1221 |
| python/hopsworks\_common/core/app\_api.py                                        |      197 |       40 |     80% |79-85, 102-116, 269, 282, 291, 293, 369-384, 388-399, 450, 460-461, 500-507, 513 |
| python/hopsworks\_common/core/constants.py                                       |       38 |        2 |     95% |     67-68 |
| python/hopsworks\_common/core/costs\_api.py                                      |       35 |        0 |    100% |           |
| python/hopsworks\_common/core/dataset.py                                         |       31 |       14 |     55% |33-37, 41-44, 48, 52, 56, 60, 64 |
| python/hopsworks\_common/core/dataset\_api.py                                    |      390 |      213 |     45% |108-170, 224, 233, 240, 245, 248-282, 306-372, 394, 419-422, 425, 494-498, 528, 543-544, 558, 571-573, 587, 632, 637, 647-655, 704, 730-739, 767-785, 811-829, 847-870, 902-921, 942-959, 975-990, 1006-1010, 1039-1091, 1110, 1134, 1158-1170, 1184-1194, 1220-1221 |
| python/hopsworks\_common/core/env\_var\_api.py                                   |       59 |        1 |     98% |       254 |
| python/hopsworks\_common/core/environment\_api.py                                |       42 |       14 |     67% |113-118, 151-156, 168-178 |
| python/hopsworks\_common/core/execution\_api.py                                  |       73 |       21 |     71% |66, 90-101, 106-112, 120-129, 132-142, 145-155, 221-223 |
| python/hopsworks\_common/core/execution\_pod\_log.py                             |       41 |        2 |     95% |   81, 102 |
| python/hopsworks\_common/core/factory\_api.py                                    |       21 |       10 |     52% |29-30, 35, 40-41, 46-47, 58, 64, 74 |
| python/hopsworks\_common/core/feature\_logging\_arrow.py                         |      197 |       27 |     86% |60, 81, 122, 130, 149, 169, 172, 183, 198, 202, 207, 209-210, 267, 269, 273, 276, 279, 333, 341-344, 349, 372, 374-381 |
| python/hopsworks\_common/core/feature\_logging\_async.py                         |       97 |       13 |     87% |66-69, 78, 97-98, 115, 120, 147-152 |
| python/hopsworks\_common/core/feature\_logging\_buffer.py                        |       50 |        2 |     96% |     30-31 |
| python/hopsworks\_common/core/feature\_logging\_file.py                          |      683 |       93 |     86% |46-47, 116-119, 142, 153-154, 160, 178-179, 210-216, 257, 266, 302-304, 312-314, 338-356, 429-430, 451-452, 560, 602-603, 612, 664-665, 689, 695-696, 701-703, 735-736, 744, 815-820, 826-827, 845, 855, 884, 899-913, 918-923, 935, 938-944, 967-969, 980, 984-985, 1003-1004, 1010, 1113-1114, 1156 |
| python/hopsworks\_common/core/git\_api.py                                        |      177 |      130 |     27% |87-122, 135-142, 157, 176, 208-211, 228-254, 257-265, 268-291, 294-316, 321-344, 347-380, 383-410, 413-440, 443-470, 473-499, 502-529, 532-556, 559-572, 578-584 |
| python/hopsworks\_common/core/git\_op\_execution\_api.py                         |        9 |        4 |     56% |     24-36 |
| python/hopsworks\_common/core/git\_provider\_api.py                              |       45 |       31 |     31% |31-34, 39-44, 49-67, 70-81, 88-96 |
| python/hopsworks\_common/core/git\_remote\_api.py                                |       35 |       25 |     29% |25, 28-43, 46-61, 64-88, 91-113 |
| python/hopsworks\_common/core/gpu\_api.py                                        |        8 |        0 |    100% |           |
| python/hopsworks\_common/core/hosts\_api.py                                      |       10 |        3 |     70% |     28-32 |
| python/hopsworks\_common/core/ingestion\_job.py                                  |       19 |        0 |    100% |           |
| python/hopsworks\_common/core/ingestion\_job\_conf.py                            |       39 |       14 |     64% |33-36, 40, 44, 48, 52, 56, 60, 64, 68, 71, 74 |
| python/hopsworks\_common/core/inode.py                                           |       41 |        6 |     85% |51, 55, 59, 67, 71, 75 |
| python/hopsworks\_common/core/job\_api.py                                        |      136 |       69 |     49% |79-92, 109-117, 132-139, 157-158, 177-187, 195-202, 214-221, 229-234, 241-244, 260-264, 293-299, 330-333, 349-360, 371-380, 392-401, 419-434, 449-459 |
| python/hopsworks\_common/core/job\_configuration.py                              |       28 |        1 |     96% |        93 |
| python/hopsworks\_common/core/kafka\_api.py                                      |       99 |       56 |     43% |70-82, 128-149, 165-170, 183-186, 196-204, 213-223, 236-243, 259-275, 292-296, 305-316, 321-330, 340, 369-389 |
| python/hopsworks\_common/core/keywords\_api.py                                   |       48 |        0 |    100% |           |
| python/hopsworks\_common/core/library\_api.py                                    |       35 |        4 |     89% |     41-54 |
| python/hopsworks\_common/core/ml\_system\_api.py                                 |       36 |        1 |     97% |        96 |
| python/hopsworks\_common/core/opensearch.py                                      |      271 |       57 |     79% |62, 93-108, 171, 288-291, 295-301, 306-313, 377-379, 388, 465-500, 543-546, 551-563, 604 |
| python/hopsworks\_common/core/opensearch\_api.py                                 |       54 |       15 |     72% |62-73, 86-87, 113-114, 135-140 |
| python/hopsworks\_common/core/project\_api.py                                    |       67 |       31 |     54% |39-43, 54-64, 76-79, 90-94, 108-115, 192-198, 201-204 |
| python/hopsworks\_common/core/project\_members\_api.py                           |       62 |        1 |     98% |        86 |
| python/hopsworks\_common/core/rest.py                                            |       18 |        1 |     94% |        63 |
| python/hopsworks\_common/core/rest\_endpoint.py                                  |      236 |       59 |     75% |48-51, 60, 63, 68, 102, 106-109, 115, 124, 130, 141, 144-146, 152, 157, 167, 182-183, 186, 201-203, 206, 271-277, 280, 305-310, 313, 325, 333, 366, 382, 385, 410-413, 418, 429-433, 440, 450 |
| python/hopsworks\_common/core/search\_api.py                                     |      133 |       63 |     53% |55-57, 63, 69, 75, 79, 82, 94-113, 120, 125, 129, 132, 144-148, 247, 309-318, 372-381, 435-444, 495-504, 524-535, 557-572, 587, 598, 604, 620-622, 626-628 |
| python/hopsworks\_common/core/secret\_api.py                                     |       61 |       16 |     74% |57-62, 92-93, 119-125, 161-163, 231-237 |
| python/hopsworks\_common/core/services\_api.py                                   |       10 |        3 |     70% |     30-35 |
| python/hopsworks\_common/core/sink\_job\_configuration.py                        |      426 |       60 |     86% |67, 95-96, 99, 106-107, 115, 119, 124, 128, 152, 186-193, 195, 203, 249, 332, 341, 439, 443, 448, 452, 463, 467, 480, 486-498, 507-515, 520, 594, 605, 614, 623, 632, 641, 650, 674, 683, 692, 702, 714, 726, 864 |
| python/hopsworks\_common/core/superset\_api.py                                   |      156 |       45 |     71% |110-111, 210, 240-252, 265, 275, 290, 303, 337-350, 363, 373, 388, 401, 433-447, 460, 470, 485, 500 |
| python/hopsworks\_common/core/tag\_schemas\_api.py                               |       50 |       20 |     60% |63-64, 80-83, 148, 159-162, 179-187 |
| python/hopsworks\_common/core/tags\_api.py                                       |       46 |        5 |     89% |67, 85-90, 175 |
| python/hopsworks\_common/core/trino\_api.py                                      |      111 |        1 |     99% |       225 |
| python/hopsworks\_common/core/trino\_catalog\_api.py                             |       63 |        2 |     97% |   130-131 |
| python/hopsworks\_common/core/type\_systems.py                                   |      277 |       45 |     84% |220, 222, 338, 391, 413-415, 421-443, 483, 504, 508-532, 539 |
| python/hopsworks\_common/core/users\_api.py                                      |      111 |        2 |     98% |  260, 262 |
| python/hopsworks\_common/core/variable\_api.py                                   |       57 |       26 |     54% |74-82, 96-101, 112, 174-182, 192-193, 201-205 |
| python/hopsworks\_common/cost.py                                                 |      116 |        8 |     93% |94, 101, 137, 141, 144, 177, 180, 273 |
| python/hopsworks\_common/decorators.py                                           |      109 |        6 |     94% |137, 163, 183, 193, 203, 213 |
| python/hopsworks\_common/engine/\_\_init\_\_.py                                  |        0 |        0 |    100% |           |
| python/hopsworks\_common/engine/alerts\_engine.py                                |       64 |        2 |     97% |   45, 101 |
| python/hopsworks\_common/engine/environment\_engine.py                           |       55 |        5 |     91% |   167-180 |
| python/hopsworks\_common/engine/execution\_engine.py                             |      108 |       62 |     43% |57-80, 83-102, 119-144, 168, 179-193, 203-209, 216, 236 |
| python/hopsworks\_common/engine/git\_engine.py                                   |       21 |        8 |     62% |     49-62 |
| python/hopsworks\_common/env\_var.py                                             |       75 |        6 |     92% |106, 112, 120, 147, 150, 158 |
| python/hopsworks\_common/environment.py                                          |      200 |        9 |     96% |65, 72, 84, 323-324, 524, 567, 670, 673 |
| python/hopsworks\_common/execution.py                                            |      162 |       18 |     89% |145, 151, 157, 163, 169, 175, 195, 200, 220, 249, 262, 302, 367, 370, 373, 378-386 |
| python/hopsworks\_common/git\_commit.py                                          |       53 |       26 |     51% |40-49, 53-60, 66, 72, 78, 84, 90, 93, 96, 99 |
| python/hopsworks\_common/git\_file\_status.py                                    |       37 |       15 |     59% |34-36, 40-45, 51, 69, 75, 78, 81, 84 |
| python/hopsworks\_common/git\_op\_execution.py                                   |       54 |       25 |     54% |44-52, 56-57, 62, 67, 72, 77, 82, 87, 92, 97, 102, 107-114 |
| python/hopsworks\_common/git\_provider.py                                        |       40 |       15 |     62% |42-45, 50-53, 59, 65, 71, 81, 84, 87, 90 |
| python/hopsworks\_common/git\_remote.py                                          |       37 |       15 |     59% |40-43, 47-52, 58, 64, 74, 77, 80, 83 |
| python/hopsworks\_common/git\_repo.py                                            |      129 |       50 |     61% |52-68, 72-77, 83, 89, 95, 101, 107, 113, 119, 125, 137, 150, 164-167, 179, 192, 205, 220, 234, 248, 262, 277, 292, 321, 336, 348, 351, 354, 357 |
| python/hopsworks\_common/gpu\_availability.py                                    |       51 |        1 |     98% |       113 |
| python/hopsworks\_common/job.py                                                  |      228 |       49 |     79% |83-90, 115, 138, 158, 164, 267, 322-326, 346-350, 362, 379, 392, 398-415, 426, 499-515, 521-522, 567, 583, 616, 619-622, 718, 721, 724, 729-731 |
| python/hopsworks\_common/job\_schedule.py                                        |       90 |       10 |     89% |112, 115, 118, 154, 160, 166, 176, 182, 188, 194 |
| python/hopsworks\_common/kafka\_schema.py                                        |       51 |       22 |     57% |36-41, 45-50, 53-55, 61, 67, 73, 79, 92, 95, 98, 101 |
| python/hopsworks\_common/kafka\_topic.py                                         |      106 |       17 |     84% |70, 97-99, 118-120, 130, 136, 146, 152, 162, 168, 183, 186, 198, 201 |
| python/hopsworks\_common/library.py                                              |       19 |        0 |    100% |           |
| python/hopsworks\_common/project.py                                              |      192 |       40 |     79% |116, 128, 140, 146, 257-260, 269-272, 281, 299, 304, 313, 322, 367, 376, 539, 548-550, 573-575, 584-586, 598, 610, 640, 683, 688, 691, 694-696, 701-702 |
| python/hopsworks\_common/project\_member.py                                      |       67 |        9 |     87% |55, 76, 82, 88, 100, 166, 169, 175, 178 |
| python/hopsworks\_common/request\_metrics.py                                     |      110 |       14 |     87% |113, 151-164 |
| python/hopsworks\_common/search\_results.py                                      |      314 |       41 |     87% |61, 110, 116, 122, 156-172, 199, 253-262, 454, 460, 466, 472, 502, 514, 520, 538, 544, 550, 556, 616 |
| python/hopsworks\_common/secret.py                                               |       61 |       11 |     82% |61, 80, 86, 92, 98, 110, 113, 116, 119-121 |
| python/hopsworks\_common/spark\_connect\_utils.py                                |       38 |        4 |     89% |66-67, 103-104 |
| python/hopsworks\_common/tag.py                                                  |      110 |        7 |     94% |73, 143, 172, 193, 205, 220, 223 |
| python/hopsworks\_common/tracing.py                                              |       96 |        5 |     95% |70-74, 210 |
| python/hopsworks\_common/triggered\_alert.py                                     |       87 |       39 |     55% |26-28, 32-35, 38, 41, 48, 51, 71-79, 83-88, 94, 100, 106, 112, 118, 124, 130, 136, 142, 145, 148, 161, 164 |
| python/hopsworks\_common/usage.py                                                |      179 |      105 |     41% |48-52, 55-57, 60-62, 65-67, 70-72, 75-85, 88-90, 93, 96, 99-101, 104, 126-127, 130-131, 134-141, 144, 170, 175, 179, 184-186, 190-191, 195-199, 203-205, 209-212, 216-224, 232-257, 261-288, 292-296 |
| python/hopsworks\_common/user.py                                                 |       92 |        4 |     96% |76, 79, 145, 148 |
| python/hopsworks\_common/util.py                                                 |      645 |       82 |     87% |76-77, 89-114, 117-120, 182, 340, 345, 465, 507-534, 539, 582, 586-589, 608, 612-615, 623-629, 634-636, 657, 687, 714, 830, 836-838, 846-851, 896, 907-918, 1003, 1034, 1340, 1365, 1391, 1396 |
| python/hopsworks\_common/version.py                                              |        1 |        0 |    100% |           |
| python/hsfs/\_\_init\_\_.py                                                      |       24 |        3 |     88% |52, 73, 83 |
| python/hsfs/builtin\_transformations.py                                          |      242 |      179 |     26% |37-41, 49-55, 76-90, 98-101, 129-138, 156-157, 190-220, 241-281, 302-343, 366-394, 416-437, 460-494, 527-557, 579-581, 600-603, 629-633, 654-659, 689-691 |
| python/hsfs/client/\_\_init\_\_.py                                               |        2 |        0 |    100% |           |
| python/hsfs/client/auth/\_\_init\_\_.py                                          |        4 |        4 |      0% |       5-8 |
| python/hsfs/client/base/\_\_init\_\_.py                                          |        2 |        0 |    100% |           |
| python/hsfs/client/exceptions.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/client/external/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/client/hopsworks/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hsfs/client/online\_store\_rest\_client/\_\_init\_\_.py                   |        4 |        4 |      0% |       5-8 |
| python/hsfs/connection.py                                                        |        2 |        0 |    100% |           |
| python/hsfs/constructor/\_\_init\_\_.py                                          |        0 |        0 |    100% |           |
| python/hsfs/constructor/external\_feature\_group\_alias.py                       |       21 |        1 |     95% |        38 |
| python/hsfs/constructor/filter.py                                                |      111 |       11 |     90% |55, 69, 78, 83, 86, 190, 193, 199, 202, 207, 210 |
| python/hsfs/constructor/fs\_query.py                                             |       77 |       19 |     75% |75, 83, 109, 127, 139, 143, 147, 153-159, 170-171, 182-183, 197-198 |
| python/hsfs/constructor/hudi\_feature\_group\_alias.py                           |       26 |        0 |    100% |           |
| python/hsfs/constructor/inference\_spine.py                                      |      193 |       10 |     95% |75-76, 94-95, 124, 258, 360, 374, 396, 412 |
| python/hsfs/constructor/join.py                                                  |       39 |        1 |     97% |        84 |
| python/hsfs/constructor/lookback.py                                              |      147 |       13 |     91% |82, 172, 174, 176, 181, 321, 323, 348, 350, 357, 360, 363, 366 |
| python/hsfs/constructor/partitioned\_by\_translator.py                           |      140 |       23 |     84% |106, 130, 132, 134, 186, 190-191, 195-196, 215, 217, 221-222, 246, 248, 284-288, 294, 301, 306 |
| python/hsfs/constructor/prediction\_times.py                                     |      210 |       16 |     92% |67, 72, 125, 135, 305, 331, 340, 347, 358, 375, 378-380, 397, 425, 504 |
| python/hsfs/constructor/prepared\_statement\_parameter.py                        |       35 |        8 |     77% |46-48, 51, 54, 57, 69, 73 |
| python/hsfs/constructor/query.py                                                 |      436 |       70 |     84% |129-131, 188, 409, 414, 452-454, 549-551, 605-610, 640, 843-849, 912-915, 959, 977, 1014, 1023-1041, 1059-1063, 1086-1088, 1094, 1097, 1102, 1105-1111, 1148, 1157, 1161, 1199, 1227-1230, 1334-1341, 1376-1377, 1379, 1388-1389, 1400 |
| python/hsfs/constructor/serving\_prepared\_statement.py                          |       65 |       19 |     71% |61-63, 66, 69, 76-82, 86, 90, 96, 100, 104, 108, 112, 136, 140 |
| python/hsfs/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsfs/core/arrow\_flight\_client.py                                        |      371 |      191 |     49% |33, 76-80, 84, 88, 94, 103, 111, 130, 137, 182, 192, 236-237, 252-260, 267-286, 294-300, 305-319, 326, 331, 366-370, 378-385, 395-398, 404-411, 414-417, 420-423, 426-428, 431-433, 441-450, 454, 461-476, 487-505, 511-517, 530-532, 545-561, 579-600, 605-607, 631-633, 655-656, 681-707, 715, 719, 724, 728, 742, 747 |
| python/hsfs/core/chart.py                                                        |      101 |       39 |     61% |47-55, 59-66, 69, 82, 87, 91, 96, 100, 105, 109, 114, 118, 123, 127, 132, 136, 141, 145, 150, 154, 159, 163, 172-174, 185-187 |
| python/hsfs/core/chart\_api.py                                                   |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/constants.py                                                    |        2 |        0 |    100% |           |
| python/hsfs/core/dashboard.py                                                    |       53 |       21 |     60% |41-43, 47-54, 57, 64, 69, 73, 78, 82, 87, 91, 100-102, 113-115 |
| python/hsfs/core/dashboard\_api.py                                               |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/data\_source.py                                                 |      242 |       21 |     91% |157, 164, 167, 202, 265, 319, 343, 413, 450, 607, 656, 668, 684, 696, 827, 831, 836, 840, 854, 902, 904 |
| python/hsfs/core/data\_source\_api.py                                            |      125 |       57 |     54% |86-97, 102-113, 120-146, 154-162, 170-184, 200-216, 226-240, 256-284, 315-329, 405-417 |
| python/hsfs/core/data\_source\_credentials\_api.py                               |       32 |        0 |    100% |           |
| python/hsfs/core/data\_source\_data.py                                           |       54 |        3 |     94% |70, 80, 116 |
| python/hsfs/core/dataset\_api/\_\_init\_\_.py                                    |        2 |        0 |    100% |           |
| python/hsfs/core/delta\_engine.py                                                |      761 |      169 |     78% |59, 82-86, 105-124, 165-166, 359, 387-446, 454-455, 463, 474, 480-515, 571-572, 693-703, 736-739, 778-780, 825, 827-836, 838-847, 849-856, 956-957, 973-977, 980-985, 998, 1094, 1101, 1154-1169, 1186-1219, 1279, 1282, 1297, 1315-1327, 1359, 1379-1380, 1382-1384, 1402, 1412-1418, 1444-1447, 1460-1465, 1475-1476, 1492-1501, 1538, 1753 |
| python/hsfs/core/deltastreamer\_jobconf.py                                       |       16 |        6 |     62% | 30, 37-44 |
| python/hsfs/core/distribution\_distance.py                                       |       54 |        2 |     96% |   165-166 |
| python/hsfs/core/distribution\_engine.py                                         |      196 |       25 |     87% |201, 207, 290, 296-302, 382-402, 442, 447, 452, 479, 482-483, 492, 498, 509 |
| python/hsfs/core/elasticsearch\_api.py                                           |       17 |        0 |    100% |           |
| python/hsfs/core/execution/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/expectation\_api.py                                             |       32 |       19 |     41% |46-61, 74-90, 100-114, 125-139, 149-162 |
| python/hsfs/core/expectation\_engine.py                                          |       25 |        2 |     92% |    61, 67 |
| python/hsfs/core/expectation\_suite\_api.py                                      |       53 |       39 |     26% |44-64, 77-100, 115-140, 150-168, 176-187 |
| python/hsfs/core/expectation\_suite\_engine.py                                   |       37 |        2 |     95% |    68, 82 |
| python/hsfs/core/explicit\_provenance.py                                         |      229 |      152 |     34% |35, 38, 41, 44, 57-63, 69, 75, 81, 84, 87, 98, 101, 109-123, 137-140, 149, 159, 175, 184, 188, 207, 210, 217-234, 238-244, 248-261, 265-282, 286-303, 307-352, 372-434, 439-486 |
| python/hsfs/core/external\_feature\_group\_engine.py                             |       65 |       18 |     72% |39, 101, 115-152, 225-227 |
| python/hsfs/core/feature\_descriptive\_statistics.py                             |      196 |       15 |     92% |120, 123-124, 140-141, 189, 233, 236, 239, 269, 281, 287, 305, 317, 329 |
| python/hsfs/core/feature\_group\_api.py                                          |      157 |       72 |     54% |55-72, 86-103, 169-175, 183-185, 202-220, 262, 346-360, 389-400, 424-435, 460-475, 493-505, 547-559, 582-599, 620-637, 661-678, 702-719 |
| python/hsfs/core/feature\_group\_base\_engine.py                                 |       93 |       23 |     75% |87, 101, 109, 113, 117, 132, 149-152, 169-172, 189-192, 209-212, 256 |
| python/hsfs/core/feature\_group\_engine.py                                       |      380 |       52 |     86% |135, 217-231, 266-267, 380, 393, 407-414, 501-535, 739-742, 757-760, 857-859, 872-874, 887-888, 928-929, 1003, 1015, 1054, 1072, 1139-1146, 1195, 1253, 1260 |
| python/hsfs/core/feature\_log\_commit\_job.py                                    |      377 |       83 |     78% |53-54, 156-178, 186-196, 219, 249, 258, 317-318, 320, 448-451, 588-595, 597-598, 624-626, 635-637, 669-710, 735, 739-749, 753 |
| python/hsfs/core/feature\_logging.py                                             |      132 |       19 |     86% |77, 131, 133, 138, 151-156, 171, 266-277, 282, 298 |
| python/hsfs/core/feature\_logging\_client.py                                     |       52 |       31 |     40% |34-37, 42-48, 62-69, 72-74, 81-85, 88, 99-103, 107-109 |
| python/hsfs/core/feature\_monitoring\_config.py                                  |      424 |       96 |     77% |57, 61, 65-67, 74-76, 79, 82, 93, 97, 100, 187-190, 261-263, 283, 288-293, 327, 333, 336, 339, 342, 435-445, 552, 555, 568, 586-592, 670, 673, 682-685, 705, 745, 784-797, 817, 843-848, 871-877, 898-903, 922, 943, 978-983, 1014, 1026, 1030, 1040-1052, 1064, 1070, 1076, 1082, 1130, 1132, 1134, 1148, 1150, 1175-1176, 1190, 1204, 1210, 1224, 1234, 1262, 1277 |
| python/hsfs/core/feature\_monitoring\_config\_api.py                             |       77 |       50 |     35% |63-70, 86-95, 108-114, 128-134, 150-156, 176-181, 191-197, 213-219, 233-240, 256-266, 290-318 |
| python/hsfs/core/feature\_monitoring\_config\_engine.py                          |      287 |       47 |     84% |170, 218, 352-374, 391-396, 405, 434-458, 474-476, 490, 536, 539, 543, 597, 755-770, 851, 955, 999-1002, 1008 |
| python/hsfs/core/feature\_monitoring\_result.py                                  |      101 |       21 |     79% |108, 111-130, 133, 136, 139-142, 148, 154, 160, 166, 172, 190 |
| python/hsfs/core/feature\_monitoring\_result\_api.py                             |       47 |       27 |     43% |62-69, 82-88, 108-116, 132-141, 155-161, 177-196 |
| python/hsfs/core/feature\_monitoring\_result\_engine.py                          |      217 |       37 |     83% |105, 158-178, 197, 280, 286, 317-323, 412, 463, 513, 534, 536, 538, 574-575, 614, 619, 812, 827-841 |
| python/hsfs/core/feature\_statistics\_config.py                                  |       52 |       10 |     81% |76-78, 82-90, 93, 96, 99 |
| python/hsfs/core/feature\_statistics\_result.py                                  |      100 |       25 |     75% |111-113, 117-136, 139, 142, 145-148, 154, 196 |
| python/hsfs/core/feature\_store\_activity\_api.py                                |       12 |       12 |      0% |     16-73 |
| python/hsfs/core/feature\_store\_api.py                                          |       12 |        3 |     75% |     32-34 |
| python/hsfs/core/feature\_view\_api.py                                           |      170 |       98 |     42% |71-72, 82-83, 105-129, 146-169, 174-180, 187-189, 194-196, 242, 272-285, 295-297, 306-309, 316-317, 328-332, 339-340, 345-348, 351-352, 357-361, 366-375, 399-413, 440-454, 467-476, 483-491, 498-506, 513-528, 538-545, 556-570 |
| python/hsfs/core/feature\_view\_engine.py                                        |      740 |      201 |     73% |128-130, 178-186, 195-203, 213, 242-243, 308, 435-441, 443-448, 450-457, 480-487, 490, 519, 667, 742-760, 764, 768, 787-788, 857-861, 918, 934, 943, 1007, 1073, 1085-1091, 1094-1097, 1151-1165, 1192-1208, 1214, 1216, 1242, 1255-1256, 1268, 1312, 1323-1324, 1342-1347, 1372, 1384, 1390, 1396, 1402, 1410, 1420-1423, 1433, 1455-1460, 1479-1486, 1494-1495, 1566-1569, 1603-1606, 1675, 1806-1837, 1843-1846, 1957, 1963, 1991, 2338, 2342, 2377, 2385, 2390, 2395, 2442, 2454, 2459, 2472, 2475-2499, 2502-2508, 2517-2520, 2523-2528, 2531-2534, 2548-2561, 2586-2594, 2601-2610 |
| python/hsfs/core/glue\_catalog.py                                                |      111 |       24 |     78% |80, 102, 116, 139, 147, 167, 192-204, 227, 274-278, 311, 313 |
| python/hsfs/core/gmail\_sync\_job.py                                             |       85 |        4 |     95% |80-81, 93, 214 |
| python/hsfs/core/great\_expectation\_engine.py                                   |       43 |        4 |     91% |75, 95-100 |
| python/hsfs/core/hosts\_api/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/hudi\_engine.py                                                 |      173 |       16 |     91% |172-178, 303, 325-330, 348-352, 361-365 |
| python/hsfs/core/iceberg\_engine.py                                              |      883 |      179 |     80% |100, 118, 126, 141-142, 151, 164, 174, 178, 181, 284-285, 317-325, 387, 389, 392-393, 401, 411-428, 545-553, 664, 666, 690, 699, 725, 745-774, 796, 804-830, 841-871, 936-952, 963, 1076, 1085-1093, 1161, 1173, 1189-1190, 1199, 1243-1244, 1366-1368, 1506, 1512-1525, 1535-1561, 1680-1684, 1690-1691, 1711-1720, 1743, 1786, 1812-1814, 1823-1841, 1877-1878, 1887, 2038-2042, 2060-2069, 2071-2073 |
| python/hsfs/core/inferred\_metadata.py                                           |       70 |        8 |     89% |54-56, 80, 96, 120, 141, 182 |
| python/hsfs/core/ingestion\_job/\_\_init\_\_.py                                  |        2 |        0 |    100% |           |
| python/hsfs/core/ingestion\_job\_conf/\_\_init\_\_.py                            |        2 |        0 |    100% |           |
| python/hsfs/core/inode/\_\_init\_\_.py                                           |        2 |        0 |    100% |           |
| python/hsfs/core/job/\_\_init\_\_.py                                             |        2 |        0 |    100% |           |
| python/hsfs/core/job\_api/\_\_init\_\_.py                                        |        2 |        0 |    100% |           |
| python/hsfs/core/job\_configuration/\_\_init\_\_.py                              |        2 |        0 |    100% |           |
| python/hsfs/core/job\_schedule/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_api/\_\_init\_\_.py                                      |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_engine.py                                                |      225 |        5 |     98% |207, 459, 480, 505, 547 |
| python/hsfs/core/keywords\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/mail\_sync\_job.py                                              |      718 |       56 |     92% |268-274, 297, 303-304, 352-353, 403-405, 412-413, 443, 466, 486, 558-563, 594-601, 645, 652-658, 865, 909, 956-957, 979-980, 1157-1158, 1370-1371, 1378-1388, 1427, 1459-1460 |
| python/hsfs/core/monitoring\_window\_config.py                                   |      130 |       29 |     78% |49, 53-55, 63, 67, 70, 152-153, 168-169, 174, 177, 180, 186, 197, 203-214, 231, 236-240, 257, 280, 289 |
| python/hsfs/core/monitoring\_window\_config\_engine.py                           |      238 |       41 |     83% |52, 72, 81, 126, 163, 227, 240-248, 261, 348-395, 688-699, 739-740, 863, 906, 924 |
| python/hsfs/core/multi\_table\_ingestion.py                                      |      156 |        7 |     96% |72, 263-264, 273, 320, 336, 412 |
| python/hsfs/core/online\_ingestion.py                                            |       94 |       13 |     86% |97, 102, 105-110, 115-116, 124, 132, 153, 184, 210, 212 |
| python/hsfs/core/online\_ingestion\_api.py                                       |       14 |        7 |     50% |52-64, 93-104 |
| python/hsfs/core/online\_ingestion\_failure.py                                   |       62 |        2 |     97% |  129, 132 |
| python/hsfs/core/online\_ingestion\_result.py                                    |       40 |       13 |     68% |64-75, 83, 95, 101 |
| python/hsfs/core/online\_store\_rest\_client\_api.py                             |       58 |       31 |     47% |38-53, 101-105, 151-155, 171-178, 199-206 |
| python/hsfs/core/online\_store\_rest\_client\_engine.py                          |      205 |       30 |     85% |72, 107, 161, 182, 221, 253, 319-322, 392-395, 407, 411, 422, 492, 510, 514, 523-527, 541, 555, 559, 573, 590, 601, 605, 609, 613, 617, 635 |
| python/hsfs/core/online\_store\_sql\_engine.py                                   |      497 |      284 |     43% |67-70, 85-86, 92, 99-100, 127, 177, 206-257, 270-292, 302-332, 346-367, 375, 436, 438, 458-464, 493, 495, 515-521, 535, 550, 558-597, 602-610, 619-633, 644-651, 659-710, 716-767, 776-788, 805-812, 882-885, 893-894, 897, 903-905, 908-919, 922, 925-936, 951-953, 963-970, 976-992, 1000-1022, 1025, 1037-1042, 1052-1067, 1080-1082, 1103, 1105, 1116-1120, 1124, 1129, 1133, 1144, 1151, 1165, 1170, 1174-1176, 1183, 1188, 1197, 1201-1218, 1222, 1226, 1230, 1234, 1238, 1242 |
| python/hsfs/core/opensearch/\_\_init\_\_.py                                      |        3 |        0 |    100% |           |
| python/hsfs/core/opensearch\_api/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hsfs/core/outlook\_sync\_job.py                                           |      136 |        5 |     96% |117, 267, 311, 319, 323 |
| python/hsfs/core/partition\_grains.py                                            |       26 |        1 |     96% |        67 |
| python/hsfs/core/partition\_transforms.py                                        |      163 |        7 |     96% |261, 405, 410, 435-438 |
| python/hsfs/core/project\_api/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/query\_constructor\_api.py                                      |       10 |        5 |     50% |     24-32 |
| python/hsfs/core/restricted\_access\_api.py                                      |       38 |        9 |     76% |85, 113-119, 151-157 |
| python/hsfs/core/schema\_validation.py                                           |      157 |       11 |     93% |39, 106, 114, 167-175, 282 |
| python/hsfs/core/search\_api.py                                                  |        3 |        0 |    100% |           |
| python/hsfs/core/services\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/share\_api.py                                                   |       78 |       78 |      0% |    20-289 |
| python/hsfs/core/sharepoint\_api.py                                              |       40 |        2 |     95% |    30, 33 |
| python/hsfs/core/sharepoint\_sync.py                                             |      305 |       28 |     91% |230, 288, 441, 491, 503, 521, 527, 533, 557, 569, 581, 587, 593, 599, 605, 611, 634-636, 652, 664, 670, 676, 682, 688, 694, 712, 718, 724 |
| python/hsfs/core/spine\_group\_engine.py                                         |       17 |       11 |     35% |     24-49 |
| python/hsfs/core/statistics\_api.py                                              |      102 |       46 |     55% |44-53, 90-119, 150-172, 203-217, 242-251, 273-292, 305, 352, 354, 394, 398, 402, 406, 410 |
| python/hsfs/core/statistics\_comparison\_config.py                               |      116 |        9 |     92% |124-126, 151, 154, 157-162, 166 |
| python/hsfs/core/statistics\_comparison\_result.py                               |       53 |       15 |     72% |55-57, 61, 70, 73, 76-79, 85, 91, 97, 103, 109 |
| python/hsfs/core/statistics\_engine.py                                           |      143 |       11 |     92% |157, 159, 218-244, 441-442, 600, 647 |
| python/hsfs/core/storage\_connector\_api.py                                      |      100 |       32 |     68% |158, 210-220, 226-227, 241, 248, 255, 267, 277, 284, 291, 299-309, 316-328, 346-366, 385-405 |
| python/hsfs/core/tag\_schemas\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/tags\_api/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_api.py                                       |       59 |       40 |     32% |31-40, 53-63, 71-81, 88-94, 102-114, 132-143, 174-185, 207-219, 231-241 |
| python/hsfs/core/training\_dataset\_engine.py                                    |       64 |        1 |     98% |       183 |
| python/hsfs/core/training\_dataset\_job\_conf.py                                 |       37 |       14 |     62% |25-28, 32, 36, 40, 44, 48, 52, 56, 60, 63, 66 |
| python/hsfs/core/transformation\_execution\_dag.py                               |      179 |       25 |     86% |78, 200, 209-220, 262, 327-328, 346-360 |
| python/hsfs/core/transformation\_function\_api.py                                |       26 |       16 |     38% |42-51, 80-95, 108-118 |
| python/hsfs/core/transformation\_function\_engine.py                             |      557 |       86 |     85% |221-222, 230-240, 264, 295-299, 321, 326-339, 373-375, 396, 410, 848, 890-891, 900, 905-910, 917-923, 988, 990, 1045-1065, 1078-1094, 1174-1184, 1239, 1344-1350, 1357, 1363-1364, 1420-1429, 1543, 1662-1667, 1719, 1804-1809, 1842 |
| python/hsfs/core/transformation\_telemetry.py                                    |      141 |       10 |     93% |152, 217, 250-251, 254-256, 324-325, 333-334 |
| python/hsfs/core/type\_systems.py                                                |        2 |        0 |    100% |           |
| python/hsfs/core/util\_sql.py                                                    |       38 |       21 |     45% |37-74, 91-106 |
| python/hsfs/core/validation\_report\_api.py                                      |       34 |       21 |     38% |44-65, 75-87, 95-113, 123-140 |
| python/hsfs/core/validation\_report\_engine.py                                   |       41 |        5 |     88% |59, 84-86, 110 |
| python/hsfs/core/validation\_result\_api.py                                      |       12 |        4 |     67% |     50-64 |
| python/hsfs/core/validation\_result\_engine.py                                   |       33 |        3 |     91% |78, 82, 119 |
| python/hsfs/core/variable\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/vector\_db\_client.py                                           |      321 |       64 |     80% |99-102, 117, 142-146, 150, 210, 310, 370, 381, 405, 441-443, 475, 501-507, 516, 526, 551, 601, 628-629, 642-661, 681-695, 698-705, 712, 718-727, 731, 735, 739-749, 753 |
| python/hsfs/core/vector\_server.py                                               |      855 |      314 |     63% |113, 144, 295, 300, 308-317, 330, 436-445, 455, 500, 526-591, 596-598, 612, 615, 624-626, 645, 648, 786-795, 815-817, 831, 834, 843-845, 868, 976, 1000, 1032, 1075-1079, 1153, 1166-1168, 1251, 1253-1255, 1257-1259, 1262, 1283, 1292, 1298, 1308, 1312, 1317, 1359-1388, 1407-1439, 1462-1522, 1558-1631, 1652-1656, 1685, 1692, 1695-1699, 1702-1704, 1708, 1710, 1712, 1714, 1729, 1731, 1736, 1743, 1747-1749, 1770-1791, 1818-1844, 1865-1891, 1900, 1907, 1916-1936, 1953-1954, 1959-1960, 1964-1965, 1969, 2024, 2042, 2127, 2149, 2161-2173, 2186-2231, 2262-2279, 2313, 2346, 2350-2352, 2356-2360, 2362, 2365, 2369, 2378, 2417, 2430-2435, 2440-2445, 2456-2461, 2464, 2533-2537, 2553, 2567, 2576, 2616-2630, 2636-2647, 2659, 2667, 2675, 2680, 2685, 2691-2709 |
| python/hsfs/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsfs/embedding.py                                                         |      155 |       40 |     74% |48, 61-63, 71, 79-100, 103, 106, 146-154, 171, 177, 183, 217, 225-232, 235, 314-318, 348-350, 354-355, 393, 401, 408 |
| python/hsfs/engine/\_\_init\_\_.py                                               |       45 |        5 |     89% |34, 41, 45, 49-50 |
| python/hsfs/engine/python.py                                                     |      985 |      114 |     88% |181, 334, 338, 370-379, 382, 445-446, 487-493, 521, 523-525, 529, 564, 570, 596, 602, 658-662, 684, 745, 786-797, 802, 805, 824, 850-854, 867-876, 916-919, 959-965, 1002-1011, 1056-1057, 1102, 1167-1170, 1200, 1203-1204, 1232-1233, 1259, 1280, 1296, 1477, 1611-1615, 1728-1729, 1767, 1816, 1882, 1901, 1904, 2066, 2109, 2334, 2403, 2425, 2443-2446, 2477, 2489-2493, 2738, 3082, 3086-3087, 3112-3116, 3118-3126, 3142-3143, 3149-3150, 3193, 3232-3233 |
| python/hsfs/engine/spark.py                                                      |     1061 |      238 |     78% |98-103, 175-176, 193-194, 201-203, 215-221, 242, 250, 280-288, 296, 361-362, 384-394, 403, 450-458, 566, 575-579, 586-618, 656-666, 705, 724-725, 728, 810-817, 1141-1142, 1253, 1286-1290, 1310-1339, 1342-1359, 1362-1391, 1527, 1532, 1551-1603, 1629, 1669, 1734-1735, 1790, 1818, 1836, 1854-1896, 1975, 1989-2008, 2012-2019, 2077, 2081, 2115, 2125-2126, 2150-2151, 2173-2174, 2177, 2260-2266, 2352-2362, 2586, 2616-2622, 2769, 2777-2791, 2795, 2918-2919, 2925-2926, 2932-2933, 2939-2940, 2953-2954, 2957, 2972 |
| python/hsfs/engine/spark\_metrics.py                                             |      115 |       26 |     77% |48-54, 78-79, 99-102, 107, 110, 128-129, 162, 185-194 |
| python/hsfs/engine/spark\_no\_metastore.py                                       |       14 |        5 |     64% | 37-47, 51 |
| python/hsfs/expectation\_suite.py                                                |      250 |       73 |     71% |53, 86, 101, 195, 241, 260, 271-276, 288, 295-310, 347, 381-385, 435-444, 474-487, 509-513, 520, 523-541, 551, 561-563, 575, 585, 595-597, 615-617, 642, 659, 663 |
| python/hsfs/feature.py                                                           |      182 |       11 |     94% |161, 201, 221, 237, 267, 300, 304, 314, 330, 368, 398 |
| python/hsfs/feature\_group.py                                                    |     1590 |      360 |     77% |171, 311, 344, 350, 594-599, 670, 882, 1014, 1042, 1065, 1091, 1103, 1126, 1140, 1154, 1167, 1184, 1202-1204, 1219, 1235, 1254, 1271, 1289-1301, 1316-1328, 1344, 1360, 1402-1404, 1424-1426, 1443-1445, 1485-1487, 1510-1512, 1539-1541, 1607-1609, 1641-1642, 1682-1697, 1731-1734, 1766-1767, 1789-1807, 1843-1846, 1889-1907, 1942-1947, 2001, 2018, 2023, 2032, 2052-2054, 2088, 2121-2123, 2174-2195, 2247-2255, 2317-2322, 2340, 2347, 2398-2403, 2458-2463, 2532-2554, 2631, 2666, 2682, 2709, 2713, 2728, 2748, 2773, 2788, 2801, 2817, 2842-2843, 2891-2892, 2930-2931, 2956-2963, 2988, 3012, 3062, 3080-3082, 3112, 3166, 3194, 3227, 3237, 3247, 3289-3290, 3297, 3330, 3339, 3342-3370, 3379, 3385-3394, 3431, 3453, 3501-3502, 3530-3531, 3795, 3904, 3909-3914, 3977, 4148, 4153, 4157, 4188, 4191-4195, 4245, 4305-4314, 4341-4345, 4438, 4442, 4446, 4526, 4713, 4722, 4980-5000, 5047, 5130, 5138, 5243, 5304, 5341, 5371, 5477, 5508, 5525, 5753-5755, 5779-5807, 5822, 5826, 5841, 5851, 5855-5856, 5866, 5874, 5889, 5906, 5966, 5972, 5974, 5979, 5983, 5987, 6207, 6229, 6236-6244, 6270, 6276, 6280, 6287-6290, 6307-6320, 6326-6331, 6355-6362, 6506-6507, 6598-6628, 6657-6658, 6778, 6796, 6805, 6829-6833, 6836, 6866-6870, 6926-6935, 6947, 6954, 6962-6970, 6973, 7004, 7006, 7053, 7203-7204, 7232-7233, 7272, 7282-7285, 7297-7299, 7302-7306, 7309, 7312 |
| python/hsfs/feature\_group\_commit.py                                            |       84 |       16 |     81% |61-64, 67, 70, 113, 121, 125, 129, 133, 137, 141, 145, 149, 153 |
| python/hsfs/feature\_group\_writer.py                                            |       17 |        0 |    100% |           |
| python/hsfs/feature\_logger.py                                                   |       17 |        3 |     82% |43, 52, 57 |
| python/hsfs/feature\_logger\_async.py                                            |      104 |       33 |     68% |52-59, 108-117, 127, 130, 158-159, 165-166, 188-196, 199-202, 205, 217-219 |
| python/hsfs/feature\_store.py                                                    |      366 |      101 |     72% |203, 222-227, 274, 276, 302, 331-348, 372, 409-410, 436-448, 470, 607-609, 635-637, 655-657, 698, 723, 743, 1000, 1284-1285, 1474-1517, 1697-1701, 1707, 1862-1880, 1977-1983, 2037, 2145, 2166, 2297-2326, 2480, 2516-2518, 2522-2525, 2567-2573, 2594, 2618, 2649-2653, 2674, 2698, 2728, 2740, 2830, 2888, 2946, 3004, 3059, 3081 |
| python/hsfs/feature\_store\_activity.py                                          |       94 |       94 |      0% |    16-183 |
| python/hsfs/feature\_view.py                                                     |     1139 |      298 |     74% |242, 377, 507-509, 538, 634-640, 679, 684-687, 823, 871, 1065, 1072, 1127, 1154-1164, 1334, 1341, 1413, 1488-1490, 1505-1535, 1599-1610, 1618-1630, 1860, 1887, 1911, 1934, 1948, 1962, 1975, 1992, 2010-2012, 2027, 2044, 2064-2068, 2086-2091, 2112, 2138, 2148, 2376-2417, 2695-2742, 3008-3064, 3581, 3848, 4020-4035, 4112-4131, 4156-4162, 4194, 4235, 4271, 4305, 4328, 4350, 4370, 4391, 4414, 4438-4440, 4461, 4494, 4521-4523, 4547-4549, 4576-4578, 4602-4604, 4657-4665, 4675-4707, 4754-4759, 4811-4816, 4878-4900, 4972-4985, 5171-5172, 5234, 5253, 5309, 5332, 5339, 5495, 5536-5539, 5595-5599, 5619, 5628-5635, 5794-5813, 5914, 5973, 5999, 6014, 6038, 6072-6073, 6096-6106, 6280-6283, 6288-6301, 6356, 6394, 6410, 6435, 6517, 6620, 6631, 6672, 6675-6681, 6712-6714, 6755-6757, 6779-6781, 6945, 6949, 6985, 7000, 7004-7021 |
| python/hsfs/ge\_expectation.py                                                   |      101 |       13 |     87% |40, 68, 120, 123, 126, 144, 158-161, 171, 186, 201 |
| python/hsfs/ge\_validation\_result.py                                            |      146 |       18 |     88% |52, 111, 157, 167, 182, 193, 199, 214, 233, 273, 286, 292, 295-301 |
| python/hsfs/hopsworks\_udf.py                                                    |      520 |       18 |     97% |87, 185, 449-451, 501-504, 524, 636, 796, 909, 915, 1100, 1124, 1142, 1144, 1374, 1671 |
| python/hsfs/online\_config.py                                                    |       91 |        1 |     99% |       137 |
| python/hsfs/serving\_key.py                                                      |       60 |        6 |     90% |57, 96, 98, 108, 123, 128 |
| python/hsfs/split\_statistics.py                                                 |       28 |        2 |     93% |    57, 65 |
| python/hsfs/statistics.py                                                        |      140 |       22 |     84% |93, 95, 97, 110, 162, 166, 170, 173, 176, 188, 192-200, 214, 220, 226, 273 |
| python/hsfs/statistics\_config.py                                                |       81 |        6 |     93% |54, 65, 67, 118, 141, 144 |
| python/hsfs/storage\_connector.py                                                |     2877 |      372 |     87% |261, 425, 510, 549, 568-577, 595, 612-615, 627-638, 655-658, 670-682, 817, 853, 855, 857, 873, 875-881, 908, 921, 1084-1092, 1135, 1140-1152, 1203, 1209-1210, 1247, 1405-1430, 1612, 1645-1657, 1671, 1735, 1816, 1856, 2024, 2045, 2120, 2122-2123, 2162-2172, 2213, 2234, 2383, 2442, 2461, 2576, 2648, 2695-2704, 2711, 2803-2812, 3063-3072, 3079, 3081, 3112-3113, 3186, 3230-3239, 3330, 3347, 3430, 3565-3566, 3651-3662, 3809, 3909, 4222-4223, 4237, 4437-4444, 4510, 4515, 4520, 4525, 4530, 4535, 4540, 4564, 4568-4587, 4598-4622, 4650, 4787, 4870, 4903, 4908, 4911-4921, 5113, 5206, 5232, 5740, 5748, 5752, 5756, 5760, 5764, 5768, 5772, 5776, 5780, 5784, 5789, 5794, 5804, 5847, 6098, 6123-6127, 6134-6142, 6150, 6165-6259, 6271-6276, 6310-6315, 6321, 6327, 6333, 6339, 6345, 6351, 6366-6368, 6387-6389, 6393-6411, 6440, 6444, 6466, 6470, 6477, 6481, 6522-6530, 6535-6537, 6545, 6549, 6553, 6557, 6561, 6565, 6569, 6573, 6577, 6581, 6641, 6644, 6653, 6656, 6660, 6662, 6666, 6670, 6674-6676, 6693, 6872, 6888-6889, 6894-6895, 6948, 6981-6982, 7018, 7031, 7086, 7092, 7156, 7162, 7174, 7180, 7186, 7192, 7198, 7201, 7279, 7286, 7365, 7383, 7423, 7577, 7586, 7659, 7683, 7723, 7874, 7883 |
| python/hsfs/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsfs/training\_dataset.py                                                 |      473 |      111 |     77% |247, 252-270, 279, 288, 297, 338, 363, 368, 378, 405, 423, 433, 435, 441, 461, 470, 481, 493, 502, 511, 520, 529, 538, 547, 556, 561, 565, 709-727, 768-774, 793-798, 803-824, 837, 853, 865, 880, 892, 907-908, 916-917, 936-941, 947-957, 959, 973, 979-988, 998, 1003, 1031, 1042, 1048, 1052, 1068, 1074, 1087, 1104, 1122-1124, 1142-1144, 1157, 1169, 1175-1183 |
| python/hsfs/training\_dataset\_feature.py                                        |       80 |        5 |     94% |59, 88, 163-166 |
| python/hsfs/training\_dataset\_split.py                                          |       54 |        7 |     87% |52, 60, 68, 76, 84, 87, 90 |
| python/hsfs/transformation\_function.py                                          |      155 |       15 |     90% |138, 169, 231-233, 241, 548, 575, 585, 616, 641-645 |
| python/hsfs/transformation\_statistics.py                                        |      139 |       16 |     88% |96, 102, 110, 116, 122, 128, 148, 172, 190, 204, 214, 220, 226, 238, 290, 297 |
| python/hsfs/usage.py                                                             |        2 |        0 |    100% |           |
| python/hsfs/user/\_\_init\_\_.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/util.py                                                              |       77 |        5 |     94% |90, 115, 244-246 |
| python/hsfs/validation\_report.py                                                |      138 |       16 |     88% |72, 104, 148, 164, 174, 227, 244, 263, 281-287, 293, 296 |
| python/hsfs/version.py                                                           |        2 |        0 |    100% |           |
| python/hsml/\_\_init\_\_.py                                                      |       14 |        2 |     86% |    37, 44 |
| python/hsml/client/\_\_init\_\_.py                                               |        2 |        0 |    100% |           |
| python/hsml/client/auth/\_\_init\_\_.py                                          |        5 |        5 |      0% |       5-9 |
| python/hsml/client/base/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hsml/client/exceptions/\_\_init\_\_.py                                    |       24 |        0 |    100% |           |
| python/hsml/client/external/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsml/client/hopsworks/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hsml/client/istio/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/client/istio/base/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hsml/client/istio/external/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hsml/client/istio/grpc/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hsml/client/istio/grpc/errors/\_\_init\_\_.py                             |        2 |        2 |      0% |       5-6 |
| python/hsml/client/istio/grpc/exceptions/\_\_init\_\_.py                         |        7 |        7 |      0% |      5-11 |
| python/hsml/client/istio/grpc/inference\_client/\_\_init\_\_.py                  |        4 |        4 |      0% |       5-8 |
| python/hsml/client/istio/hopsworks/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsml/client/istio/utils/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hsml/client/istio/utils/infer\_type.py                                    |        2 |        0 |    100% |           |
| python/hsml/client/istio/utils/numpy\_codec/\_\_init\_\_.py                      |        3 |        3 |      0% |       5-7 |
| python/hsml/connection.py                                                        |        2 |        0 |    100% |           |
| python/hsml/constants.py                                                         |        2 |        0 |    100% |           |
| python/hsml/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsml/core/dataset\_api/\_\_init\_\_.py                                    |        3 |        0 |    100% |           |
| python/hsml/core/explicit\_provenance.py                                         |      189 |       86 |     54% |65, 71, 77, 80, 87, 95-109, 138, 176, 186, 202, 205, 228-229, 249-286, 290-319, 335-356, 364-379, 384-409 |
| python/hsml/core/hdfs\_api.py                                                    |       21 |       14 |     33% |24-28, 52-76, 88 |
| python/hsml/core/huggingface\_api.py                                             |       30 |        9 |     70% |79, 81, 83, 85, 107-108, 123-125 |
| python/hsml/core/model\_api.py                                                   |       87 |       57 |     34% |40-50, 70-80, 108-124, 146-174, 182-191, 204-217, 228-239, 322-346, 365-389 |
| python/hsml/core/model\_registry\_api.py                                         |       24 |       16 |     33% | 27, 38-62 |
| python/hsml/core/model\_serving\_api.py                                          |       54 |       17 |     69% |47, 60-72, 81-82, 129-136 |
| python/hsml/core/openai\_client.py                                               |       23 |        0 |    100% |           |
| python/hsml/core/serving\_api.py                                                 |      303 |       93 |     69% |64-79, 91-101, 115-130, 146-157, 170-179, 242-245, 346-355, 452, 514, 552-560, 568-575, 588-596, 609-619, 715, 797-800, 811-819, 830-835, 874-897 |
| python/hsml/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsml/default\_predictor.py                                                |      698 |       60 |     91% |94-96, 114-118, 127-129, 162-170, 210-211, 227, 358-359, 403, 409, 415-424, 429-430, 513, 614, 687-689, 741, 783, 814, 868, 1063-1064, 1080-1082, 1135, 1235-1237, 1569, 1571, 1574-1575, 1587, 1592-1593, 1606, 1610 |
| python/hsml/deployable\_component.py                                             |       56 |        5 |     91% |73, 95, 105, 115, 125 |
| python/hsml/deployable\_component\_logs.py                                       |       85 |        7 |     92% |114, 184, 189, 193, 198, 202, 205 |
| python/hsml/deployment.py                                                        |      523 |       69 |     87% |593, 600, 624-634, 641, 661-666, 698, 705, 714, 864-872, 972, 1036, 1205, 1235, 1256, 1262, 1272, 1310, 1336, 1342, 1354, 1360, 1364, 1374, 1384, 1388, 1394, 1404, 1416, 1420, 1426, 1430, 1440, 1444, 1450, 1454, 1460, 1464, 1470, 1474, 1480, 1484, 1503, 1507, 1513, 1517, 1523, 1529, 1535, 1539, 1552, 1556, 1611, 1615, 1651, 1655, 1658-1663 |
| python/hsml/deployment\_logging\_config.py                                       |      171 |       16 |     91% |124, 139, 144-150, 200, 235, 239, 263, 269, 273, 283, 293, 303, 313, 323 |
| python/hsml/deployment\_memory\_config.py                                        |      146 |       22 |     85% |109, 123, 165, 190, 209-211, 214, 245, 249, 259, 269, 275, 279, 289, 295, 299, 305, 309, 315, 319, 322 |
| python/hsml/deployment\_schema.py                                                |      881 |       44 |     95% |95, 113, 116, 237, 345-348, 361, 639-640, 733, 810-811, 876, 893, 915, 917, 948, 954, 956, 960, 1035, 1038, 1157, 1202, 1232, 1248, 1257, 1260, 1273, 1277, 1286, 1296, 1365, 1372-1374, 1380, 1498, 1588, 1710, 1777, 1889 |
| python/hsml/deployment\_tracing\_config.py                                       |      176 |       12 |     93% |88, 179-181, 184, 222, 232, 253, 284, 298, 312, 324 |
| python/hsml/deployment\_version.py                                               |      203 |       20 |     90% |216, 234, 252, 258, 264, 282, 327, 333, 339, 345, 357, 363, 369, 375, 381, 387, 393, 405, 411, 414 |
| python/hsml/engine/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/engine/local\_engine.py                                              |       53 |       33 |     38% |36-42, 45-46, 49, 58-76, 94-113, 116-118, 121-123, 126, 129-132 |
| python/hsml/engine/model\_engine.py                                              |      402 |      220 |     45% |82-102, 108-135, 145, 157-158, 168, 174-185, 203-232, 238-245, 258-278, 319-403, 416-539, 592-594, 612, 617, 663-664, 669, 681, 684-685, 717-724, 735-738, 740, 746, 753-754, 770-771, 781-782, 814-816, 840-865, 868-885, 898, 907, 917, 926, 935, 943, 960, 977 |
| python/hsml/engine/serving\_engine.py                                            |      799 |      226 |     72% |54-55, 121-123, 170, 179, 188, 196, 208, 210, 215-232, 250, 267, 284-286, 295, 297, 302-340, 383-422, 425-445, 448-479, 492-497, 500-501, 512-541, 547-554, 557-612, 634, 636, 638, 686-713, 729-742, 768, 820, 1046, 1053, 1064-1071, 1074-1101, 1295, 1455, 1465-1467, 1588, 1682, 1707, 1711, 1720, 1725-1760, 1774, 1779, 1787, 1793, 1931-1933 |
| python/hsml/inference\_batcher.py                                                |       77 |       10 |     87% |52, 83-85, 88, 108, 118, 128, 138, 141 |
| python/hsml/inference\_endpoint.py                                               |       84 |        7 |     92% |54, 67, 109-111, 133, 155 |
| python/hsml/inference\_logger.py                                                 |       66 |        8 |     88% |51, 93-95, 98, 114, 124, 127 |
| python/hsml/kafka\_topic/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/llm/\_\_init\_\_.py                                                  |        0 |        0 |    100% |           |
| python/hsml/llm/model.py                                                         |        8 |        2 |     75% |     72-73 |
| python/hsml/llm/predictor.py                                                     |       14 |        0 |    100% |           |
| python/hsml/llm/signature.py                                                     |       13 |        4 |     69% |     79-94 |
| python/hsml/model.py                                                             |      357 |       53 |     85% |145-150, 156-171, 303, 344, 499, 545, 578-579, 653, 684, 699, 728-731, 793-801, 812, 847, 878, 888, 898, 908, 918, 928, 938, 942, 952, 962, 966, 972, 976, 988, 998, 1010, 1020, 1030, 1039, 1048, 1057, 1063, 1067, 1075 |
| python/hsml/model\_registry.py                                                   |      201 |       26 |     87% |82-83, 103-108, 136, 163-172, 178, 184, 190, 323, 328, 342, 369, 421, 436, 448, 454, 460, 466, 472, 478, 481-486 |
| python/hsml/model\_schema.py                                                     |       21 |        5 |     76% |60, 67, 70-76 |
| python/hsml/model\_serving.py                                                    |      370 |       34 |     91% |42-43, 114, 142, 186-190, 193-201, 210, 249, 464, 550, 760, 826, 1151, 1160, 1274, 1368-1369, 1404, 1411, 1421, 1426-1427, 1454, 1456, 1458, 1460, 1462 |
| python/hsml/predictor.py                                                         |      833 |       77 |     91% |68, 144, 244, 423, 466, 474-476, 611, 764, 774, 784, 794-795, 805, 809, 844, 854, 868, 878, 888, 904, 908, 922, 936, 948, 960, 970, 980, 990, 1028-1031, 1068, 1104, 1124, 1140, 1144, 1154, 1164, 1174, 1311, 1321-1335, 1359-1364, 1386, 1416, 1450-1452, 1474, 1506, 1508, 1512, 1535-1536, 1546, 1567-1568, 1598, 1603-1604, 1636, 1645, 1675 |
| python/hsml/predictor\_state.py                                                  |       87 |       14 |     84% |53, 83-103, 160 |
| python/hsml/predictor\_state\_condition.py                                       |       50 |        7 |     86% |42, 62-64, 67, 70, 97 |
| python/hsml/python/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/python/endpoint.py                                                   |       10 |        0 |    100% |           |
| python/hsml/python/feature\_view\_endpoint.py                                    |        9 |        1 |     89% |        32 |
| python/hsml/python/model.py                                                      |        8 |        0 |    100% |           |
| python/hsml/python/predictor.py                                                  |        9 |        5 |     44% |     25-33 |
| python/hsml/python/signature.py                                                  |       13 |        0 |    100% |           |
| python/hsml/resources.py                                                         |      167 |       11 |     93% |57, 81, 107, 132, 181, 200, 235, 239, 269, 279, 282 |
| python/hsml/scaling\_config.py                                                   |      389 |       32 |     92% |98, 102, 124, 184, 297, 480-482, 486, 536, 553-562, 595, 608, 653, 669, 689, 727, 737, 747, 757, 767, 785, 820, 834, 866, 881 |
| python/hsml/schema.py                                                            |       30 |        3 |     90% |72, 79, 82 |
| python/hsml/sklearn/\_\_init\_\_.py                                              |        0 |        0 |    100% |           |
| python/hsml/sklearn/model.py                                                     |        8 |        0 |    100% |           |
| python/hsml/sklearn/predictor.py                                                 |        7 |        3 |     57% |     25-28 |
| python/hsml/sklearn/signature.py                                                 |       13 |        4 |     69% |     79-94 |
| python/hsml/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsml/tensorflow/\_\_init\_\_.py                                           |        0 |        0 |    100% |           |
| python/hsml/tensorflow/model.py                                                  |        8 |        2 |     75% |     72-73 |
| python/hsml/tensorflow/predictor.py                                              |        9 |        5 |     44% |     25-33 |
| python/hsml/tensorflow/signature.py                                              |       13 |        4 |     69% |     79-94 |
| python/hsml/torch/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/torch/model.py                                                       |        8 |        2 |     75% |     72-73 |
| python/hsml/torch/predictor.py                                                   |        9 |        5 |     44% |     25-33 |
| python/hsml/torch/signature.py                                                   |       13 |        4 |     69% |     79-94 |
| python/hsml/transformer.py                                                       |       94 |        9 |     90% |33, 79, 138-141, 171, 179, 185 |
| python/hsml/triton/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/triton/model.py                                                      |        8 |        2 |     75% |     72-73 |
| python/hsml/triton/predictor.py                                                  |       22 |        0 |    100% |           |
| python/hsml/triton/signature.py                                                  |       13 |        4 |     69% |     82-97 |
| python/hsml/util/\_\_init\_\_.py                                                 |       18 |       18 |      0% |      5-22 |
| python/hsml/utils/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/utils/local\_paths.py                                                |       50 |        1 |     98% |       133 |
| python/hsml/utils/schema/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hsml/utils/schema/column.py                                               |        7 |        0 |    100% |           |
| python/hsml/utils/schema/columnar\_schema.py                                     |       61 |        0 |    100% |           |
| python/hsml/utils/schema/tensor.py                                               |        8 |        0 |    100% |           |
| python/hsml/utils/schema/tensor\_schema.py                                       |       34 |        0 |    100% |           |
| python/hsml/version.py                                                           |        2 |        2 |      0% |     17-22 |
| **TOTAL**                                                                        | **58884** | **13423** | **77%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/logicalclocks/hopsworks-api/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/logicalclocks/hopsworks-api/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Flogicalclocks%2Fhopsworks-api%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.