# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                             |    Stmts |     Miss |   Cover |   Missing |
|--------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| python/hopsworks/\_\_init\_\_.py                                                 |      251 |       81 |     68% |78-80, 84-86, 96-98, 150, 153, 156, 165-167, 278, 306, 310, 318, 343, 369-371, 398-403, 417, 447, 453, 463, 465-497, 510, 542-544, 589-606, 617-619, 644-646, 657-659, 677, 687 |
| python/hopsworks/alert/\_\_init\_\_.py                                           |        6 |        6 |      0% |      5-10 |
| python/hopsworks/alert\_receiver/\_\_init\_\_.py                                 |        6 |        6 |      0% |      5-10 |
| python/hopsworks/app/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/cli/\_\_init\_\_.py                                             |        0 |        0 |    100% |           |
| python/hopsworks/cli/\_\_main\_\_.py                                             |        3 |        3 |      0% |       3-7 |
| python/hopsworks/cli/auth.py                                                     |       45 |        2 |     96% |   40, 106 |
| python/hopsworks/cli/commands/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks/cli/commands/agent.py                                           |      184 |       47 |     74% |39-40, 70-86, 144-145, 171-172, 197-198, 231-232, 237-238, 241-242, 330-331, 333-334, 340-352, 362-363, 365-366, 386, 389-390, 403-404, 438-446, 459 |
| python/hopsworks/cli/commands/alert.py                                           |      321 |      117 |     64% |68-69, 107-108, 125-126, 129-135, 175-176, 178-179, 202-203, 228-250, 297-312, 340-341, 344-345, 347-348, 363-370, 436-437, 454-455, 457-458, 483-484, 502-509, 537-538, 540-541, 567-575, 607-608, 612-613, 639-647, 675-688, 701-702, 710-715, 720-726, 735, 739, 741 |
| python/hopsworks/cli/commands/app.py                                             |      298 |       33 |     89% |46-47, 132, 175, 180-181, 233-234, 386, 391, 401, 408, 410, 437, 449-450, 514-515, 545-546, 560-561, 587, 594-595, 612-613, 638, 649, 656-657, 661, 676 |
| python/hopsworks/cli/commands/context.py                                         |       88 |       24 |     73% |112, 124-131, 134-137, 140-145, 148-155, 187, 231-232 |
| python/hopsworks/cli/commands/datasource.py                                      |      384 |       61 |     84% |70-71, 75-78, 90-105, 109, 159, 282, 284, 286, 340, 449, 1292, 1303-1304, 1322-1323, 1325-1326, 1342-1356, 1369-1379, 1410-1411, 1424-1425, 1428-1429, 1441, 1458-1459 |
| python/hopsworks/cli/commands/deployment.py                                      |      285 |      107 |     62% |36-37, 68-69, 73-89, 118-159, 163-166, 170-178, 191-196, 216, 241, 338, 342-343, 345, 351-366, 379-380, 394, 428-429, 454-455, 495, 499-500, 505-506, 509-510, 604-605, 607-608, 614-630, 640-641, 643-644, 664, 667-668, 710-711, 713 |
| python/hopsworks/cli/commands/env.py                                             |       72 |       21 |     71% |32, 48-52, 95-115, 172-173, 207-208, 210 |
| python/hopsworks/cli/commands/fg.py                                              |      487 |      168 |     66% |138-139, 151-152, 258-260, 291, 293, 383-415, 505-554, 604-613, 619-620, 624, 626, 669-708, 743, 746-747, 790, 792, 805-806, 832-833, 837-863, 907-908, 911-914, 946-947, 950-953, 992-993, 1017-1018, 1037-1038, 1063-1064, 1085-1086, 1095-1101, 1116-1117, 1127-1129, 1142, 1149, 1173-1193, 1215, 1224-1232, 1236-1239 |
| python/hopsworks/cli/commands/files.py                                           |      103 |       31 |     70% |43-44, 77-78, 115-116, 141-148, 181-190, 214-221, 237, 241-242 |
| python/hopsworks/cli/commands/fv.py                                              |      317 |      127 |     60% |57-58, 91-101, 171-187, 283-284, 290-291, 294-295, 319-320, 381-410, 442-443, 448, 486-508, 536, 542-543, 559-564, 585-590, 607-612, 627-639, 658-663, 682-687, 696-700, 710, 721, 730-750, 754-762 |
| python/hopsworks/cli/commands/git.py                                             |       81 |       10 |     88% |141-142, 144-147, 203-212, 225-228, 251-252 |
| python/hopsworks/cli/commands/init.py                                            |        8 |        0 |    100% |           |
| python/hopsworks/cli/commands/job.py                                             |      375 |      176 |     53% |32, 35-42, 52, 54-58, 78-79, 108-109, 113-126, 130-143, 191-206, 275-327, 379, 388-389, 398, 447-468, 481-489, 536, 539-542, 549-550, 554-555, 564-565, 568-569, 720-721, 724, 737-751, 767-768, 784-791, 804-809, 826-831, 846-851, 866-872, 878, 882, 890-891, 893, 900-901, 907, 916-919 |
| python/hopsworks/cli/commands/login.py                                           |       27 |       17 |     37% |     53-84 |
| python/hopsworks/cli/commands/logout.py                                          |       18 |        1 |     94% |        33 |
| python/hopsworks/cli/commands/model.py                                           |      163 |       54 |     67% |66-69, 72, 91-107, 112, 207, 212-215, 219-223, 238-239, 272-273, 291-298, 339-346, 351, 356, 360-361 |
| python/hopsworks/cli/commands/project.py                                         |       51 |        6 |     88% |63-64, 107-118 |
| python/hopsworks/cli/commands/search.py                                          |       48 |        4 |     92% |110, 128-129, 187 |
| python/hopsworks/cli/commands/session.py                                         |      753 |      294 |     61% |51-53, 169, 247-248, 293-294, 331-332, 363-367, 374, 426, 456-460, 503, 599, 615-621, 804-805, 833, 845, 880-893, 910, 951-996, 1052, 1063-1070, 1074, 1080, 1116, 1127-1128, 1146-1153, 1162-1190, 1197, 1201, 1220, 1258, 1262, 1273-1286, 1289-1295, 1308-1330, 1354-1397, 1411-1437, 1467-1473, 1502-1503, 1526-1534, 1567-1570, 1580-1589, 1599-1609, 1619-1636, 1654-1720, 1736-1781, 1806-1830 |
| python/hopsworks/cli/commands/setup.py                                           |      248 |       50 |     80% |77-84, 105, 113-114, 137, 175-193, 228, 368-371, 385, 389-390, 403, 449, 479-482, 511-512, 598-600, 605, 607, 609, 629, 632 |
| python/hopsworks/cli/commands/skills.py                                          |      163 |       27 |     83% |47, 80-81, 83, 167-168, 172, 192-214, 291, 298, 358-361, 389-390, 394 |
| python/hopsworks/cli/commands/superset.py                                        |      138 |       38 |     72% |51-52, 91-101, 116-122, 141-142, 155, 199-200, 217, 220-221, 255, 278-279, 295-301, 328-330, 336-337, 345 |
| python/hopsworks/cli/commands/td.py                                              |      191 |       77 |     60% |53-54, 60-61, 135-136, 144, 187-215, 242-256, 335-347, 372-377, 404-409, 418-419, 430, 433-435, 446, 450-451, 458-466 |
| python/hopsworks/cli/commands/transformation.py                                  |       84 |       13 |     85% |36-37, 98, 103-104, 115-116, 173, 180-181, 184, 191, 194 |
| python/hopsworks/cli/commands/trino.py                                           |      134 |       80 |     40% |45, 64-65, 70-71, 76-85, 90-95, 108-110, 114-116, 127-154, 220-231, 251, 264, 279-285, 300-305, 319-330 |
| python/hopsworks/cli/commands/update.py                                          |       49 |       15 |     69% |54, 69-70, 73, 76, 87-96, 107-108 |
| python/hopsworks/cli/config.py                                                   |      155 |       22 |     86% |75, 89, 105, 109-110, 119-121, 140, 144, 149-151, 168-169, 245, 247, 331-337 |
| python/hopsworks/cli/git\_sync.py                                                |      375 |      140 |     63% |73-74, 139-163, 172-197, 224-225, 231-232, 258-259, 264-287, 336, 358-360, 367-382, 387-397, 402-412, 421-480, 505, 522-524, 538-545, 548-550, 575-576, 581-583, 612, 663-664, 666, 684, 697, 701-702, 709-715, 722, 740 |
| python/hopsworks/cli/joinspec.py                                                 |       18 |        0 |    100% |           |
| python/hopsworks/cli/lineage.py                                                  |       33 |        4 |     88% | 55, 62-64 |
| python/hopsworks/cli/main.py                                                     |      114 |        9 |     92% |87, 103, 122-123, 265, 343-345, 357 |
| python/hopsworks/cli/output.py                                                   |       85 |        8 |     91% |73, 95, 112-113, 124-133, 247, 258 |
| python/hopsworks/cli/scaffold.py                                                 |      121 |        4 |     97% |177, 205, 208, 308 |
| python/hopsworks/cli/session.py                                                  |       49 |        8 |     84% |70, 82-83, 99, 101-103, 121 |
| python/hopsworks/cli/templates/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/cli/terminal\_api.py                                            |       18 |        3 |     83% |     93-97 |
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
| python/hopsworks\_common/app.py                                                  |      303 |       17 |     94% |137, 146, 184, 242, 258, 270, 276, 357, 374, 453-456, 500-501, 566, 572 |
| python/hopsworks\_common/client/\_\_init\_\_.py                                  |       87 |       15 |     83% |45-61, 69, 82, 95, 128, 136, 146, 186, 194 |
| python/hopsworks\_common/client/auth.py                                          |       36 |       12 |     67% |39-40, 52, 55-56, 77-83 |
| python/hopsworks\_common/client/base.py                                          |      231 |       53 |     77% |74-79, 87-92, 96, 100, 104-105, 116, 119-120, 162, 192, 196, 225, 354, 359, 365, 386, 393-405, 431, 441-443, 451-455, 459-464, 468-475 |
| python/hopsworks\_common/client/exceptions.py                                    |      148 |        8 |     95% |48-50, 56, 155-159, 167, 179 |
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
| python/hopsworks\_common/client/istio/utils/infer\_type.py                       |      330 |      234 |     29% |55, 75-105, 121-123, 126-129, 140, 151, 159, 217, 222, 231, 234-241, 259-316, 320-336, 374-375, 379-389, 404-417, 425-452, 465-474, 500-507, 518, 529, 534, 545, 556, 564, 572-579, 595-652, 683-692, 696-706, 717-727, 740-755, 763-790 |
| python/hopsworks\_common/client/istio/utils/numpy\_codec.py                      |       35 |       29 |     17% |24-39, 44-70 |
| python/hopsworks\_common/client/online\_store\_rest\_client.py                   |      338 |       65 |     81% |73, 92-98, 109-113, 115, 151, 155, 180-187, 202, 204, 208, 211, 217, 225, 244, 276, 285, 301, 306-309, 331, 339, 344, 349, 355, 418, 481-483, 500, 560-566, 580, 606, 613, 620, 629, 635, 645, 650-651, 662-675, 701, 706, 714 |
| python/hopsworks\_common/command.py                                              |       25 |        4 |     84% |     49-52 |
| python/hopsworks\_common/connection.py                                           |      292 |       96 |     67% |170-174, 182-186, 194-198, 221-223, 268, 300, 315-326, 336, 349, 363, 369-374, 398-466, 549, 553, 557-559, 648, 662, 667, 671, 676, 680, 685, 689, 694, 698, 703, 712, 716, 720, 725, 734-737, 742, 747, 750-751, 754 |
| python/hopsworks\_common/constants.py                                            |      200 |        2 |     99% |    25, 28 |
| python/hopsworks\_common/core/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks\_common/core/alerts\_api.py                                     |      270 |      170 |     37% |178-181, 213-216, 245-248, 281-291, 325-336, 375-387, 423-436, 477-491, 540-574, 624-662, 712-737, 778-802, 830-834, 868-873, 916-981, 1005-1009, 1052-1084, 1117-1121, 1133-1136, 1144-1189, 1201-1205, 1208-1221 |
| python/hopsworks\_common/core/app\_api.py                                        |      152 |       38 |     75% |52-58, 75-89, 196, 199, 209, 216, 218, 280-295, 299-310, 364-371, 377 |
| python/hopsworks\_common/core/constants.py                                       |       36 |        2 |     94% |     67-68 |
| python/hopsworks\_common/core/dataset.py                                         |       31 |       14 |     55% |33-37, 41-44, 48, 52, 56, 60, 64 |
| python/hopsworks\_common/core/dataset\_api.py                                    |      391 |      213 |     46% |109-171, 225, 234, 241, 246, 249-283, 307-373, 395, 420-423, 426, 495-499, 529, 544-545, 559, 572-574, 588, 633, 638, 648-656, 705, 731-740, 768-786, 812-830, 848-871, 903-922, 943-960, 976-991, 1007-1011, 1040-1092, 1111, 1135, 1159-1171, 1185-1195, 1221-1222 |
| python/hopsworks\_common/core/env\_var\_api.py                                   |       59 |        1 |     98% |       254 |
| python/hopsworks\_common/core/environment\_api.py                                |       42 |       14 |     67% |113-118, 151-156, 168-178 |
| python/hopsworks\_common/core/execution\_api.py                                  |       73 |       21 |     71% |66, 90-101, 106-112, 120-129, 132-142, 145-155, 221-223 |
| python/hopsworks\_common/core/execution\_pod\_log.py                             |       41 |        2 |     95% |   81, 102 |
| python/hopsworks\_common/core/feature\_logging\_arrow.py                         |      197 |       27 |     86% |60, 81, 122, 130, 149, 169, 172, 183, 198, 202, 207, 209-210, 267, 269, 273, 276, 279, 333, 341-344, 349, 372, 374-381 |
| python/hopsworks\_common/core/feature\_logging\_async.py                         |       97 |       13 |     87% |66-69, 78, 97-98, 115, 120, 147-152 |
| python/hopsworks\_common/core/feature\_logging\_buffer.py                        |       50 |        2 |     96% |     30-31 |
| python/hopsworks\_common/core/feature\_logging\_file.py                          |      683 |      133 |     81% |46-47, 116-119, 142, 153-154, 160, 178-179, 210-216, 257, 266, 302-304, 312-314, 338-356, 429-430, 451-452, 560, 602-603, 612, 664-665, 689, 695-696, 701-703, 735-736, 744, 815-820, 826-827, 845, 855, 884, 899-913, 918-923, 935, 938-944, 967-969, 980, 984-985, 1003-1004, 1010, 1024-1026, 1029-1098, 1111-1122, 1156 |
| python/hopsworks\_common/core/git\_api.py                                        |      177 |      130 |     27% |87-122, 135-142, 157, 176, 208-211, 228-254, 257-265, 268-291, 294-316, 321-344, 347-380, 383-410, 413-440, 443-470, 473-499, 502-529, 532-556, 559-572, 578-584 |
| python/hopsworks\_common/core/git\_op\_execution\_api.py                         |        9 |        4 |     56% |     24-36 |
| python/hopsworks\_common/core/git\_provider\_api.py                              |       45 |       31 |     31% |31-34, 39-44, 49-67, 70-81, 88-96 |
| python/hopsworks\_common/core/git\_remote\_api.py                                |       35 |       25 |     29% |25, 28-43, 46-61, 64-88, 91-113 |
| python/hopsworks\_common/core/hosts\_api.py                                      |       10 |        3 |     70% |     28-32 |
| python/hopsworks\_common/core/ingestion\_job.py                                  |       19 |        0 |    100% |           |
| python/hopsworks\_common/core/ingestion\_job\_conf.py                            |       39 |       14 |     64% |33-36, 40, 44, 48, 52, 56, 60, 64, 68, 71, 74 |
| python/hopsworks\_common/core/inode.py                                           |       41 |        6 |     85% |51, 55, 59, 67, 71, 75 |
| python/hopsworks\_common/core/job\_api.py                                        |      136 |       69 |     49% |79-92, 109-117, 132-139, 157-158, 177-187, 195-202, 214-221, 229-234, 241-244, 260-264, 293-299, 330-333, 349-360, 371-380, 392-401, 419-434, 449-459 |
| python/hopsworks\_common/core/job\_configuration.py                              |       28 |        1 |     96% |        93 |
| python/hopsworks\_common/core/kafka\_api.py                                      |       99 |       56 |     43% |70-82, 128-149, 165-170, 183-186, 196-204, 213-223, 236-243, 259-275, 292-296, 305-316, 321-330, 340, 369-389 |
| python/hopsworks\_common/core/keywords\_api.py                                   |       48 |        0 |    100% |           |
| python/hopsworks\_common/core/library\_api.py                                    |       15 |        4 |     73% |     41-54 |
| python/hopsworks\_common/core/opensearch.py                                      |      265 |       64 |     76% |58-61, 76-102, 165, 211, 275-278, 282-288, 293-300, 364-366, 375, 452-487, 530-533, 538-550, 591 |
| python/hopsworks\_common/core/opensearch\_api.py                                 |       54 |       15 |     72% |62-73, 86-87, 113-114, 135-140 |
| python/hopsworks\_common/core/project\_api.py                                    |       58 |       32 |     45% |39-43, 54-64, 75-79, 90-94, 108-115, 170-176, 179-182 |
| python/hopsworks\_common/core/project\_members\_api.py                           |       57 |        1 |     98% |        86 |
| python/hopsworks\_common/core/rest.py                                            |       18 |        1 |     94% |        63 |
| python/hopsworks\_common/core/rest\_endpoint.py                                  |      236 |       59 |     75% |48-51, 60, 63, 68, 102, 106-109, 115, 124, 130, 141, 144-146, 152, 157, 167, 182-183, 186, 201-203, 206, 271-277, 280, 305-310, 313, 325, 333, 366, 382, 385, 410-413, 418, 429-433, 440, 450 |
| python/hopsworks\_common/core/search\_api.py                                     |      133 |       82 |     38% |54-56, 62, 68, 74, 78, 81, 93-112, 119, 124, 128, 131, 143-147, 246, 308-317, 371-380, 434-443, 494-503, 519-534, 552-571, 585-633 |
| python/hopsworks\_common/core/secret\_api.py                                     |       61 |       16 |     74% |57-62, 92-93, 119-125, 161-163, 231-237 |
| python/hopsworks\_common/core/services\_api.py                                   |       10 |        3 |     70% |     30-35 |
| python/hopsworks\_common/core/sink\_job\_configuration.py                        |      379 |       57 |     85% |67, 95-96, 99, 106-107, 115, 119, 124, 128, 152, 186-193, 195, 203, 249, 287, 296, 385, 389, 394, 398, 404-416, 425-433, 438, 507, 518, 527, 536, 545, 554, 563, 587, 596, 605, 615, 627, 639, 768 |
| python/hopsworks\_common/core/superset\_api.py                                   |      156 |       45 |     71% |110-111, 210, 240-252, 265, 275, 290, 303, 337-350, 363, 373, 388, 401, 433-447, 460, 470, 485, 500 |
| python/hopsworks\_common/core/tag\_schemas\_api.py                               |       50 |       20 |     60% |63-64, 80-83, 148, 159-162, 179-187 |
| python/hopsworks\_common/core/tags\_api.py                                       |       47 |        9 |     81% |62-68, 86-91, 176 |
| python/hopsworks\_common/core/trino\_api.py                                      |       95 |        1 |     99% |       225 |
| python/hopsworks\_common/core/trino\_catalog\_api.py                             |       61 |        2 |     97% |   125-126 |
| python/hopsworks\_common/core/type\_systems.py                                   |      277 |       45 |     84% |220, 222, 338, 391, 413-415, 421-443, 483, 504, 508-532, 539 |
| python/hopsworks\_common/core/users\_api.py                                      |      111 |        2 |     98% |  260, 262 |
| python/hopsworks\_common/core/variable\_api.py                                   |       57 |       32 |     44% |74-82, 96-101, 112, 150-159, 174-182, 192-193, 201-205 |
| python/hopsworks\_common/decorators.py                                           |      101 |        6 |     94% |132, 158, 168, 178, 188, 198 |
| python/hopsworks\_common/engine/\_\_init\_\_.py                                  |        0 |        0 |    100% |           |
| python/hopsworks\_common/engine/alerts\_engine.py                                |       64 |        2 |     97% |   45, 101 |
| python/hopsworks\_common/engine/environment\_engine.py                           |       52 |        8 |     85% |136-141, 146-159 |
| python/hopsworks\_common/engine/execution\_engine.py                             |      108 |       62 |     43% |57-80, 83-102, 119-144, 168, 179-193, 203-209, 216, 236 |
| python/hopsworks\_common/engine/git\_engine.py                                   |       21 |        8 |     62% |     49-62 |
| python/hopsworks\_common/env\_var.py                                             |       75 |        6 |     92% |106, 112, 120, 147, 150, 158 |
| python/hopsworks\_common/environment.py                                          |       75 |        5 |     93% |65, 72, 84, 252, 255 |
| python/hopsworks\_common/execution.py                                            |      162 |       19 |     88% |75, 145, 151, 157, 163, 169, 175, 195, 200, 220, 249, 262, 302, 367, 370, 373, 378-386 |
| python/hopsworks\_common/git\_commit.py                                          |       53 |       26 |     51% |40-49, 53-60, 66, 72, 78, 84, 90, 93, 96, 99 |
| python/hopsworks\_common/git\_file\_status.py                                    |       37 |       15 |     59% |34-36, 40-45, 51, 69, 75, 78, 81, 84 |
| python/hopsworks\_common/git\_op\_execution.py                                   |       54 |       25 |     54% |44-52, 56-57, 62, 67, 72, 77, 82, 87, 92, 97, 102, 107-114 |
| python/hopsworks\_common/git\_provider.py                                        |       40 |       15 |     62% |42-45, 50-53, 59, 65, 71, 81, 84, 87, 90 |
| python/hopsworks\_common/git\_remote.py                                          |       37 |       15 |     59% |40-43, 47-52, 58, 64, 74, 77, 80, 83 |
| python/hopsworks\_common/git\_repo.py                                            |      129 |       50 |     61% |52-68, 72-77, 83, 89, 95, 101, 107, 113, 119, 125, 137, 150, 164-167, 179, 192, 205, 220, 234, 248, 262, 277, 292, 321, 336, 348, 351, 354, 357 |
| python/hopsworks\_common/job.py                                                  |      228 |       49 |     79% |83-90, 115, 138, 158, 164, 267, 322-326, 346-350, 362, 379, 392, 398-415, 426, 499-515, 521-522, 567, 583, 616, 619-622, 718, 721, 724, 729-731 |
| python/hopsworks\_common/job\_schedule.py                                        |       90 |       10 |     89% |112, 115, 118, 154, 160, 166, 176, 182, 188, 194 |
| python/hopsworks\_common/kafka\_schema.py                                        |       51 |       22 |     57% |36-41, 45-50, 53-55, 61, 67, 73, 79, 92, 95, 98, 101 |
| python/hopsworks\_common/kafka\_topic.py                                         |      106 |       17 |     84% |70, 97-99, 118-120, 130, 136, 146, 152, 162, 168, 183, 186, 198, 201 |
| python/hopsworks\_common/library.py                                              |       19 |        0 |    100% |           |
| python/hopsworks\_common/project.py                                              |      164 |       42 |     74% |105, 111, 117, 123, 129, 135, 236-239, 248-251, 260, 278, 283, 292, 301, 310, 319, 423, 432-434, 457-459, 468-470, 482, 494, 524, 567, 572, 575, 578-580, 585-586 |
| python/hopsworks\_common/project\_member.py                                      |       67 |        9 |     87% |55, 76, 82, 88, 100, 166, 169, 175, 178 |
| python/hopsworks\_common/search\_results.py                                      |      299 |       41 |     86% |61, 110, 116, 122, 156-172, 199, 253-262, 448, 454, 460, 466, 496, 508, 514, 532, 538, 544, 550, 589 |
| python/hopsworks\_common/secret.py                                               |       61 |       11 |     82% |61, 80, 86, 92, 98, 110, 113, 116, 119-121 |
| python/hopsworks\_common/spark\_connect\_utils.py                                |       38 |        4 |     89% |66-67, 103-104 |
| python/hopsworks\_common/tag.py                                                  |      105 |        7 |     93% |73, 131, 160, 181, 193, 208, 211 |
| python/hopsworks\_common/triggered\_alert.py                                     |       87 |       39 |     55% |26-28, 32-35, 38, 41, 48, 51, 71-79, 83-88, 94, 100, 106, 112, 118, 124, 130, 136, 142, 145, 148, 161, 164 |
| python/hopsworks\_common/usage.py                                                |      179 |      105 |     41% |48-52, 55-57, 60-62, 65-67, 70-72, 75-85, 88-90, 93, 96, 99-101, 104, 126-127, 130-131, 134-141, 144, 170, 175, 179, 184-186, 190-191, 195-199, 203-205, 209-212, 216-224, 232-257, 261-288, 292-296 |
| python/hopsworks\_common/user.py                                                 |       92 |        4 |     96% |76, 79, 145, 148 |
| python/hopsworks\_common/util.py                                                 |      638 |       82 |     87% |76-77, 89-114, 117-120, 182, 340, 345, 465, 507-534, 539, 582, 586-589, 608, 612-615, 623-629, 634-636, 656, 684, 711, 823, 829-831, 839-844, 889, 900-911, 996, 1027, 1333, 1358, 1384, 1389 |
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
| python/hsfs/constructor/query.py                                                 |      426 |       71 |     83% |129-131, 188, 409, 414, 421, 451-453, 529-531, 585-590, 620, 823-829, 892-895, 939, 957, 994, 1003-1021, 1039-1043, 1066-1068, 1074, 1077, 1082, 1085-1091, 1128, 1137, 1141, 1179, 1207-1210, 1314-1321, 1356-1357, 1359, 1368-1369, 1380 |
| python/hsfs/constructor/serving\_prepared\_statement.py                          |       65 |       19 |     71% |61-63, 66, 69, 76-82, 86, 90, 96, 100, 104, 108, 112, 136, 140 |
| python/hsfs/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsfs/core/arrow\_flight\_client.py                                        |      359 |      188 |     48% |32, 75-79, 83, 87, 93, 99, 107, 113, 120, 165, 175, 217-218, 233-241, 248-267, 275-281, 286-300, 307, 312, 347-351, 359-366, 376-379, 385-392, 395-398, 401-404, 407-409, 412-414, 422-431, 435, 442-454, 465-483, 489-495, 508-510, 523-539, 557-578, 583-585, 609-611, 633-634, 659-685, 693, 697, 702, 706, 720, 725 |
| python/hsfs/core/chart.py                                                        |      101 |       39 |     61% |47-55, 59-66, 69, 82, 87, 91, 96, 100, 105, 109, 114, 118, 123, 127, 132, 136, 141, 145, 150, 154, 159, 163, 172-174, 185-187 |
| python/hsfs/core/chart\_api.py                                                   |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/constants.py                                                    |        2 |        0 |    100% |           |
| python/hsfs/core/dashboard.py                                                    |       53 |       21 |     60% |41-43, 47-54, 57, 64, 69, 73, 78, 82, 87, 91, 100-102, 113-115 |
| python/hsfs/core/dashboard\_api.py                                               |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/data\_source.py                                                 |      207 |       23 |     89% |155, 162, 165, 200, 210, 253, 263, 292, 316, 378, 415, 572, 621, 633, 649, 661, 666, 670, 675, 679, 693, 741, 743 |
| python/hsfs/core/data\_source\_api.py                                            |      108 |       64 |     41% |43-55, 60-71, 76-87, 94-120, 128-136, 144-158, 174-190, 200-214, 230-258, 264-278, 283-297, 373-385 |
| python/hsfs/core/data\_source\_data.py                                           |       49 |        3 |     94% |68, 78, 114 |
| python/hsfs/core/dataset\_api/\_\_init\_\_.py                                    |        2 |        0 |    100% |           |
| python/hsfs/core/delta\_engine.py                                                |      755 |      169 |     78% |59, 82-86, 105-124, 165-166, 357, 385-444, 452-453, 461, 472, 478-513, 569-570, 691-701, 734-737, 768-770, 804, 806-815, 817-826, 828-835, 935-936, 952-956, 959-964, 977, 1073, 1080, 1133-1148, 1165-1198, 1258, 1261, 1276, 1294-1306, 1338, 1358-1359, 1361-1363, 1381, 1391-1397, 1423-1426, 1439-1444, 1454-1455, 1471-1480, 1517, 1732 |
| python/hsfs/core/deltastreamer\_jobconf.py                                       |       16 |        6 |     62% | 30, 37-44 |
| python/hsfs/core/distribution\_distance.py                                       |       54 |        2 |     96% |   165-166 |
| python/hsfs/core/distribution\_engine.py                                         |      196 |       25 |     87% |201, 207, 290, 296-302, 382-402, 442, 447, 452, 479, 482-483, 492, 498, 509 |
| python/hsfs/core/execution/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/expectation\_api.py                                             |       32 |       19 |     41% |46-61, 74-90, 100-114, 125-139, 149-162 |
| python/hsfs/core/expectation\_engine.py                                          |       25 |        2 |     92% |    61, 67 |
| python/hsfs/core/expectation\_suite\_api.py                                      |       53 |       39 |     26% |44-64, 77-100, 115-140, 150-168, 176-187 |
| python/hsfs/core/expectation\_suite\_engine.py                                   |       37 |        2 |     95% |    68, 82 |
| python/hsfs/core/explicit\_provenance.py                                         |      229 |      152 |     34% |35, 38, 41, 44, 57-63, 69, 75, 81, 84, 87, 98, 101, 109-123, 137-140, 149, 159, 175, 184, 188, 207, 210, 217-234, 238-244, 248-261, 265-282, 286-303, 307-352, 372-434, 439-486 |
| python/hsfs/core/external\_feature\_group\_engine.py                             |       63 |       18 |     71% |36, 98, 112-149, 222-224 |
| python/hsfs/core/feature\_descriptive\_statistics.py                             |      196 |       15 |     92% |120, 123-124, 140-141, 189, 233, 236, 239, 269, 281, 287, 305, 317, 329 |
| python/hsfs/core/feature\_group\_api.py                                          |      146 |       92 |     37% |51-68, 82-99, 174-180, 188-190, 207-234, 250-270, 283-293, 312-326, 355-366, 390-401, 426-441, 459-471, 489-501, 524-541, 562-579, 603-620, 644-661 |
| python/hsfs/core/feature\_group\_base\_engine.py                                 |       93 |       23 |     75% |87, 101, 109, 113, 117, 132, 149-152, 169-172, 189-192, 209-212, 256 |
| python/hsfs/core/feature\_group\_engine.py                                       |      380 |       52 |     86% |135, 217-231, 266-267, 380, 393, 407-414, 501-535, 739-742, 757-760, 857-859, 872-874, 887-888, 928-929, 1003, 1015, 1054, 1072, 1139-1146, 1195, 1253, 1260 |
| python/hsfs/core/feature\_log\_commit\_job.py                                    |      377 |       83 |     78% |53-54, 156-178, 186-196, 219, 249, 258, 317-318, 320, 448-451, 588-595, 597-598, 624-626, 635-637, 669-710, 735, 739-749, 753 |
| python/hsfs/core/feature\_logging.py                                             |      116 |       21 |     82% |76, 130, 132, 137, 150-155, 160, 165, 170, 225-236, 241, 257 |
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
| python/hsfs/core/feature\_view\_engine.py                                        |      715 |      227 |     68% |128-130, 178-186, 195-203, 213, 242-243, 308, 435-441, 443-448, 450-457, 480-487, 490, 519, 667, 742-760, 764, 768, 787-788, 857-861, 918, 934, 943, 1007, 1073, 1085-1091, 1094-1097, 1151-1165, 1192-1208, 1214, 1216, 1242, 1255-1256, 1268, 1312, 1323-1324, 1342-1347, 1372, 1384, 1390, 1396, 1402, 1410, 1420-1423, 1433, 1455-1460, 1479-1486, 1494-1495, 1566-1569, 1603-1606, 1675, 1806-1837, 1843-1846, 1945-2040, 2292, 2296, 2327-2380, 2386-2390, 2393-2417, 2420-2426, 2435-2438, 2441-2446, 2449-2452, 2466-2479, 2504-2512, 2519-2528 |
| python/hsfs/core/glue\_catalog.py                                                |      111 |       24 |     78% |80, 102, 116, 139, 147, 167, 192-204, 227, 274-278, 311, 313 |
| python/hsfs/core/great\_expectation\_engine.py                                   |       43 |        4 |     91% |75, 95-100 |
| python/hsfs/core/hosts\_api/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/hudi\_engine.py                                                 |      173 |       16 |     91% |172-178, 303, 325-330, 348-352, 361-365 |
| python/hsfs/core/iceberg\_engine.py                                              |      866 |      187 |     78% |99, 117, 125, 140-141, 150, 163, 173, 177, 180, 283, 293-301, 363, 365, 368-369, 377, 387-404, 521-529, 640, 642, 666, 675, 701, 721-750, 772, 780-806, 817-847, 912-928, 939, 1052, 1061-1069, 1137, 1149, 1165-1166, 1175, 1218-1219, 1341-1343, 1477-1480, 1485-1498, 1508-1534, 1634-1653, 1673-1682, 1705, 1748, 1774-1776, 1785-1803, 1839-1840, 1849, 2000-2004, 2022-2031, 2033-2035 |
| python/hsfs/core/inferred\_metadata.py                                           |       70 |        8 |     89% |54-56, 80, 96, 120, 141, 182 |
| python/hsfs/core/ingestion\_job/\_\_init\_\_.py                                  |        2 |        0 |    100% |           |
| python/hsfs/core/ingestion\_job\_conf/\_\_init\_\_.py                            |        2 |        0 |    100% |           |
| python/hsfs/core/inode/\_\_init\_\_.py                                           |        2 |        0 |    100% |           |
| python/hsfs/core/job/\_\_init\_\_.py                                             |        2 |        0 |    100% |           |
| python/hsfs/core/job\_api/\_\_init\_\_.py                                        |        2 |        0 |    100% |           |
| python/hsfs/core/job\_configuration/\_\_init\_\_.py                              |        2 |        0 |    100% |           |
| python/hsfs/core/job\_schedule/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_api/\_\_init\_\_.py                                      |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_engine.py                                                |      220 |        6 |     97% |208, 458, 479, 485, 500, 542 |
| python/hsfs/core/keywords\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/monitoring\_window\_config.py                                   |      130 |       29 |     78% |49, 53-55, 63, 67, 70, 152-153, 168-169, 174, 177, 180, 186, 197, 203-214, 231, 236-240, 257, 280, 289 |
| python/hsfs/core/monitoring\_window\_config\_engine.py                           |      238 |       41 |     83% |52, 72, 81, 126, 163, 227, 240-248, 261, 348-395, 688-699, 739-740, 863, 906, 924 |
| python/hsfs/core/multi\_table\_ingestion.py                                      |      152 |        7 |     95% |72, 258-259, 268, 315, 331, 407 |
| python/hsfs/core/online\_ingestion.py                                            |       91 |       31 |     66% |97, 102, 105-110, 115-116, 124, 132, 153, 161, 181-221 |
| python/hsfs/core/online\_ingestion\_api.py                                       |       14 |        7 |     50% |52-64, 93-104 |
| python/hsfs/core/online\_ingestion\_failure.py                                   |       62 |        2 |     97% |  129, 132 |
| python/hsfs/core/online\_ingestion\_result.py                                    |       40 |       18 |     55% |48-50, 64-75, 83, 95, 101, 111, 117 |
| python/hsfs/core/online\_store\_rest\_client\_api.py                             |       58 |       31 |     47% |38-53, 101-105, 151-155, 171-178, 199-206 |
| python/hsfs/core/online\_store\_rest\_client\_engine.py                          |      202 |       31 |     85% |72, 107, 161, 182, 221, 253, 310-313, 336, 383-386, 398, 402, 413, 483, 501, 505, 514-518, 532, 546, 550, 564, 581, 592, 596, 600, 604, 608, 626 |
| python/hsfs/core/online\_store\_sql\_engine.py                                   |      498 |      306 |     39% |67-70, 85-86, 92, 97-100, 126-150, 171, 204-255, 268-290, 300-330, 344-365, 373, 437, 439, 459-465, 494, 496, 516-522, 536, 551, 559-598, 603-611, 620-634, 645-652, 660-711, 717-768, 777-789, 806-813, 883-886, 894-895, 898, 901-906, 909-920, 923, 926-937, 952-954, 964-971, 977-993, 1001-1023, 1026, 1038-1043, 1053-1068, 1081-1083, 1104, 1106, 1117-1121, 1125, 1130, 1134, 1145, 1152, 1166, 1171, 1175-1177, 1184, 1189, 1198, 1202-1219, 1223, 1227, 1231, 1235, 1239, 1243 |
| python/hsfs/core/opensearch/\_\_init\_\_.py                                      |        3 |        0 |    100% |           |
| python/hsfs/core/opensearch\_api/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hsfs/core/partition\_grains.py                                            |       26 |        1 |     96% |        67 |
| python/hsfs/core/partition\_transforms.py                                        |      163 |        7 |     96% |261, 405, 410, 435-438 |
| python/hsfs/core/project\_api/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/query\_constructor\_api.py                                      |       10 |        5 |     50% |     24-32 |
| python/hsfs/core/restricted\_access\_api.py                                      |       38 |        9 |     76% |85, 113-119, 151-157 |
| python/hsfs/core/schema\_validation.py                                           |      157 |       11 |     93% |39, 106, 114, 167-175, 282 |
| python/hsfs/core/search\_api.py                                                  |        3 |        0 |    100% |           |
| python/hsfs/core/services\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/share\_api.py                                                   |       78 |       78 |      0% |    20-289 |
| python/hsfs/core/spine\_group\_engine.py                                         |       17 |       11 |     35% |     24-49 |
| python/hsfs/core/statistics\_api.py                                              |      102 |       46 |     55% |44-53, 90-119, 150-172, 203-217, 242-251, 273-292, 305, 352, 354, 394, 398, 402, 406, 410 |
| python/hsfs/core/statistics\_comparison\_config.py                               |      116 |        9 |     92% |124-126, 151, 154, 157-162, 166 |
| python/hsfs/core/statistics\_comparison\_result.py                               |       53 |       15 |     72% |55-57, 61, 70, 73, 76-79, 85, 91, 97, 103, 109 |
| python/hsfs/core/statistics\_engine.py                                           |      143 |       11 |     92% |157, 159, 218-244, 441-442, 600, 647 |
| python/hsfs/core/storage\_connector\_api.py                                      |       56 |       39 |     30% |35-46, 60-69, 90-100, 121-126, 139, 166-176, 181-191, 198-210, 228-248, 267-287 |
| python/hsfs/core/tag\_schemas\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/tags\_api/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_api.py                                       |       59 |       40 |     32% |31-40, 53-63, 71-81, 88-94, 102-114, 132-143, 174-185, 207-219, 231-241 |
| python/hsfs/core/training\_dataset\_engine.py                                    |       64 |        1 |     98% |       183 |
| python/hsfs/core/training\_dataset\_job\_conf.py                                 |       37 |       14 |     62% |25-28, 32, 36, 40, 44, 48, 52, 56, 60, 63, 66 |
| python/hsfs/core/transformation\_execution\_dag.py                               |      179 |       25 |     86% |78, 200, 209-220, 262, 327-328, 346-360 |
| python/hsfs/core/transformation\_function\_api.py                                |       26 |       16 |     38% |42-51, 80-95, 108-118 |
| python/hsfs/core/transformation\_function\_engine.py                             |      525 |       84 |     84% |221-222, 230-240, 264, 295-299, 321, 326-339, 373-375, 396, 410, 819, 861-862, 871, 876-881, 888-894, 983-1003, 1016-1032, 1112-1122, 1177, 1282-1288, 1295, 1301-1302, 1358-1367, 1481, 1600-1605, 1657, 1742-1747, 1780 |
| python/hsfs/core/type\_systems.py                                                |        2 |        0 |    100% |           |
| python/hsfs/core/util\_sql.py                                                    |       38 |       21 |     45% |37-74, 91-106 |
| python/hsfs/core/validation\_report\_api.py                                      |       34 |       21 |     38% |44-65, 75-87, 95-113, 123-140 |
| python/hsfs/core/validation\_report\_engine.py                                   |       41 |        5 |     88% |59, 84-86, 110 |
| python/hsfs/core/validation\_result\_api.py                                      |       12 |        4 |     67% |     50-64 |
| python/hsfs/core/validation\_result\_engine.py                                   |       33 |        3 |     91% |78, 82, 119 |
| python/hsfs/core/variable\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/vector\_db\_client.py                                           |      295 |       63 |     79% |93-96, 111, 136-140, 144, 212, 289, 300, 324, 360-362, 394, 420-426, 435, 445, 470, 520, 547-548, 561-580, 600-614, 617-624, 631, 637-646, 650, 654, 658-668, 672 |
| python/hsfs/core/vector\_server.py                                               |      793 |      354 |     55% |110, 141, 268-328, 399-408, 417-431, 441, 467-532, 537-539, 553, 556, 565-567, 586, 589, 726-735, 755-757, 771, 774, 783-785, 808, 916, 940, 972, 1014-1018, 1090, 1103-1105, 1186, 1188-1190, 1192-1194, 1197, 1217, 1226, 1232, 1242, 1246, 1251, 1271-1300, 1319-1351, 1374-1415, 1451-1504, 1525-1529, 1558, 1565, 1568-1572, 1575-1577, 1581, 1583, 1585, 1587, 1602, 1604, 1609, 1616, 1620-1622, 1643-1664, 1691-1717, 1738-1764, 1772-1786, 1789-1809, 1826-1827, 1832-1833, 1837-1838, 1842, 1867-1912, 1917-1929, 1942-1987, 2018-2035, 2069, 2102, 2106-2108, 2112-2116, 2118, 2121, 2125, 2134, 2173, 2186-2191, 2196-2201, 2212-2217, 2220, 2263-2266, 2272, 2290-2294, 2310, 2324, 2333, 2373-2387, 2393-2404, 2416, 2420-2443, 2448-2466 |
| python/hsfs/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsfs/embedding.py                                                         |      155 |       40 |     74% |48, 61-63, 71, 79-100, 103, 106, 146-154, 171, 177, 183, 217, 225-232, 235, 314-318, 348-350, 354-355, 393, 401, 408 |
| python/hsfs/engine/\_\_init\_\_.py                                               |       45 |        5 |     89% |34, 41, 45, 49-50 |
| python/hsfs/engine/python.py                                                     |      972 |      126 |     87% |181, 334, 338, 370-379, 382, 445-446, 487-493, 521, 523-525, 529, 564, 570, 596, 602, 658-662, 684, 745, 786-797, 802, 805, 824, 850-854, 867-876, 916-919, 959-965, 1002-1011, 1056-1057, 1102, 1167-1170, 1200, 1203-1204, 1232-1233, 1259, 1278, 1294, 1475, 1609-1613, 1726-1727, 1765, 1814, 1879-1891, 1899, 1902, 2064, 2107, 2330, 2360-2364, 2403, 2421-2424, 2455, 2467-2471, 2713, 3046, 3050-3051, 3076-3080, 3082-3090, 3106-3107, 3113-3114, 3158, 3197-3198 |
| python/hsfs/engine/spark.py                                                      |     1030 |      236 |     77% |94-99, 171-172, 189-190, 197-199, 211-217, 238, 246, 276-284, 292, 357-358, 380-390, 399, 446-454, 562, 571-575, 582-614, 652-662, 701, 720-721, 724, 806-813, 1137-1138, 1249, 1282-1286, 1306-1335, 1338-1355, 1358-1387, 1523, 1528, 1547-1599, 1625, 1665, 1741, 1769, 1787, 1805-1847, 1926, 1940-1959, 1963-1970, 2028, 2032, 2066, 2076-2077, 2101-2102, 2124-2125, 2128, 2210-2216, 2302-2312, 2536, 2566-2572, 2719, 2727-2741, 2745, 2861-2862, 2868-2869, 2875-2876, 2882-2883, 2896-2897, 2900, 2915 |
| python/hsfs/engine/spark\_metrics.py                                             |      115 |       26 |     77% |48-54, 78-79, 99-102, 107, 110, 128-129, 162, 185-194 |
| python/hsfs/engine/spark\_no\_metastore.py                                       |       14 |        5 |     64% | 37-47, 51 |
| python/hsfs/expectation\_suite.py                                                |      250 |       73 |     71% |53, 86, 101, 195, 241, 260, 271-276, 288, 295-310, 347, 381-385, 435-444, 474-487, 509-513, 520, 523-541, 551, 561-563, 575, 585, 595-597, 615-617, 642, 659, 663 |
| python/hsfs/feature.py                                                           |      182 |       11 |     94% |161, 201, 221, 237, 267, 300, 304, 314, 330, 368, 398 |
| python/hsfs/feature\_group.py                                                    |     1577 |      359 |     77% |171, 311, 344, 350, 594-599, 670, 882, 1014, 1042, 1065, 1091, 1103, 1126, 1140, 1154, 1167, 1184, 1202-1204, 1219, 1235, 1254, 1271, 1289-1301, 1316-1328, 1344, 1360, 1402-1404, 1424-1426, 1443-1445, 1485-1487, 1510-1512, 1539-1541, 1607-1609, 1641-1642, 1682-1697, 1731-1734, 1766-1767, 1789-1807, 1843-1846, 1889-1907, 1942-1947, 2001, 2018, 2023, 2032, 2052-2054, 2088, 2121-2123, 2174-2195, 2247-2255, 2317-2322, 2340, 2347, 2398-2403, 2458-2463, 2532-2554, 2631, 2666, 2682, 2709, 2713, 2728, 2748, 2773, 2788, 2801, 2817, 2842-2843, 2891-2892, 2930-2931, 2956-2963, 2988, 3012, 3062, 3080-3082, 3112, 3166, 3194, 3227, 3237, 3247, 3289-3290, 3297, 3330, 3339, 3342-3370, 3379, 3385-3394, 3431, 3453, 3501-3502, 3530-3531, 3795, 3902, 3907-3912, 3975, 4135, 4140, 4144, 4175, 4178-4182, 4230, 4290-4299, 4326-4330, 4423, 4427, 4431, 4511, 4698, 4707, 4965-4985, 5032, 5115, 5123, 5228, 5289, 5326, 5356, 5462, 5493, 5510, 5738-5740, 5764-5792, 5807, 5811, 5826, 5836, 5840-5841, 5851, 5859, 5874, 5891, 5951, 5957, 5959, 5964, 5968, 5972, 6192, 6214, 6221-6229, 6255, 6261, 6265, 6272-6275, 6292-6305, 6311-6316, 6340-6347, 6489-6490, 6581-6611, 6640-6641, 6758, 6776, 6785, 6809-6813, 6844-6848, 6904-6913, 6925, 6932, 6940-6948, 6951, 6982, 6984, 7031, 7143-7144, 7172-7173, 7212, 7222-7225, 7237-7239, 7242-7246, 7249, 7252 |
| python/hsfs/feature\_group\_commit.py                                            |       84 |       16 |     81% |61-64, 67, 70, 113, 121, 125, 129, 133, 137, 141, 145, 149, 153 |
| python/hsfs/feature\_group\_writer.py                                            |       17 |        0 |    100% |           |
| python/hsfs/feature\_logger.py                                                   |       17 |        3 |     82% |43, 52, 57 |
| python/hsfs/feature\_logger\_async.py                                            |      104 |       33 |     68% |52-59, 108-117, 127, 130, 158-159, 165-166, 188-196, 199-202, 205, 217-219 |
| python/hsfs/feature\_store.py                                                    |      363 |      106 |     71% |203, 222-227, 274, 276, 302, 331-348, 372, 409-410, 436-448, 470, 497, 525, 564-566, 595-597, 623-625, 643-645, 686, 711, 731, 988, 1272-1273, 1462-1505, 1685-1689, 1695, 1850-1868, 1965-1971, 2025, 2133, 2154, 2285-2314, 2468, 2504-2506, 2510-2513, 2555-2561, 2582, 2606, 2637-2641, 2662, 2686, 2716, 2728, 2818, 2876, 2934, 2992, 3047, 3069 |
| python/hsfs/feature\_store\_activity.py                                          |       94 |       94 |      0% |    16-183 |
| python/hsfs/feature\_view.py                                                     |     1085 |      286 |     74% |207, 342, 472-474, 503, 596-602, 641, 646-649, 785, 833, 1022, 1029, 1083, 1109-1119, 1284, 1291, 1363, 1429, 1438-1440, 1455-1485, 1549-1560, 1568-1580, 1810, 1837, 1861, 1884, 1898, 1912, 1925, 1942, 1960-1962, 1977, 1994, 2014-2018, 2036-2041, 2062, 2088, 2098, 2326-2367, 2645-2692, 2958-3014, 3531, 3798, 3970-3985, 4062-4081, 4106-4112, 4144, 4185, 4221, 4255, 4278, 4300, 4320, 4341, 4364, 4388-4390, 4411, 4444, 4471-4473, 4497-4499, 4526-4528, 4552-4554, 4607-4615, 4625-4657, 4704-4709, 4761-4766, 4828-4850, 4922-4935, 5117-5118, 5180, 5199, 5255, 5278, 5285, 5441, 5482-5485, 5541-5545, 5565, 5574-5581, 5738-5757, 5810, 5864, 5890, 5905, 5929, 5958-5959, 5982-5992, 6166-6169, 6174-6187, 6242, 6280, 6296, 6321, 6403, 6506, 6517, 6558, 6561-6567, 6598-6600, 6611, 6635, 6641-6643, 6668-6670, 6834, 6838, 6874 |
| python/hsfs/ge\_expectation.py                                                   |      101 |       13 |     87% |40, 68, 120, 123, 126, 144, 158-161, 171, 186, 201 |
| python/hsfs/ge\_validation\_result.py                                            |      146 |       18 |     88% |52, 111, 157, 167, 182, 193, 199, 214, 233, 273, 286, 292, 295-301 |
| python/hsfs/hopsworks\_udf.py                                                    |      520 |       18 |     97% |87, 185, 449-451, 501-504, 524, 636, 796, 909, 915, 1100, 1124, 1142, 1144, 1374, 1671 |
| python/hsfs/online\_config.py                                                    |       91 |        1 |     99% |       137 |
| python/hsfs/serving\_key.py                                                      |       60 |        6 |     90% |57, 96, 98, 108, 123, 128 |
| python/hsfs/split\_statistics.py                                                 |       28 |        2 |     93% |    57, 65 |
| python/hsfs/statistics.py                                                        |      140 |       22 |     84% |93, 95, 97, 110, 162, 166, 170, 173, 176, 188, 192-200, 214, 220, 226, 273 |
| python/hsfs/statistics\_config.py                                                |       81 |        6 |     93% |54, 65, 67, 118, 141, 144 |
| python/hsfs/storage\_connector.py                                                |     2026 |      405 |     80% |157, 181-188, 274, 319-343, 394, 433, 452-461, 465, 479, 496-499, 511-522, 539-542, 554-566, 585-591, 618, 621, 623, 625, 640, 642-648, 665-672, 685, 837-843, 886, 891-903, 954, 960-961, 998, 1156-1181, 1363, 1396-1408, 1422, 1486, 1567, 1607, 1775, 1796, 1871, 1873-1874, 1913-1923, 1964, 1985, 2134, 2193, 2212, 2327, 2399, 2446-2455, 2462, 2554-2563, 2819-2828, 2835, 2837, 2868-2869, 2942, 2986-2995, 3086, 3103, 3186, 3321-3322, 3407-3418, 3535, 3540, 3545, 3568-3569, 3583, 3599, 3767-3778, 3818-3827, 3836, 3841, 3846, 3851, 3856, 3861, 3866, 3869-3887, 3890, 3894-3913, 3924-3948, 3976, 4035, 4039, 4043, 4047, 4051, 4055, 4059, 4063, 4067, 4071, 4075, 4079, 4084, 4089, 4116, 4367, 4392-4396, 4403-4411, 4419, 4434-4528, 4540-4545, 4579-4584, 4590, 4596, 4602, 4608, 4614, 4620, 4635-4637, 4656-4658, 4662-4680, 4704-4705, 4709, 4713, 4716, 4726-4727, 4732-4742, 4746, 4750, 4753, 4791-4799, 4802-4806, 4814, 4818, 4822, 4826, 4830, 4834, 4838, 4842, 4846, 4850, 4853-4871, 4909-4913, 4922, 4924-4931, 4935, 4939, 4943-4945, 4948-4959, 4962, 5141, 5157-5158, 5163-5164, 5217, 5250-5251, 5287, 5300 |
| python/hsfs/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsfs/training\_dataset.py                                                 |      473 |      111 |     77% |247, 252-270, 279, 288, 297, 338, 363, 368, 378, 405, 423, 433, 435, 441, 461, 470, 481, 493, 502, 511, 520, 529, 538, 547, 556, 561, 565, 709-727, 768-774, 793-798, 803-824, 837, 853, 865, 880, 892, 907-908, 916-917, 936-941, 947-957, 959, 973, 979-988, 998, 1003, 1031, 1042, 1048, 1052, 1068, 1074, 1087, 1104, 1122-1124, 1142-1144, 1157, 1169, 1175-1183 |
| python/hsfs/training\_dataset\_feature.py                                        |       80 |        5 |     94% |59, 88, 163-166 |
| python/hsfs/training\_dataset\_split.py                                          |       54 |        7 |     87% |52, 60, 68, 76, 84, 87, 90 |
| python/hsfs/transformation\_function.py                                          |      155 |       15 |     90% |138, 169, 231-233, 241, 548, 575, 585, 616, 641-645 |
| python/hsfs/transformation\_statistics.py                                        |      139 |       16 |     88% |96, 102, 110, 116, 122, 128, 148, 172, 190, 204, 214, 220, 226, 238, 290, 297 |
| python/hsfs/usage.py                                                             |        2 |        0 |    100% |           |
| python/hsfs/user/\_\_init\_\_.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/util.py                                                              |       77 |        9 |     88% |90, 115, 241-255 |
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
| python/hsml/core/huggingface\_api.py                                             |       28 |       18 |     36% |37-38, 75-86, 103-104, 119-121 |
| python/hsml/core/model\_api.py                                                   |       87 |       57 |     34% |40-50, 70-80, 108-124, 146-174, 182-191, 204-217, 228-239, 322-346, 365-389 |
| python/hsml/core/model\_registry\_api.py                                         |       24 |       16 |     33% | 27, 38-62 |
| python/hsml/core/model\_serving\_api.py                                          |       53 |       20 |     62% |38-46, 51-63, 72-73, 120-127 |
| python/hsml/core/serving\_api.py                                                 |      241 |       92 |     62% |62-77, 89-99, 113-128, 144-155, 168-177, 240-243, 271-280, 377, 426-434, 442-449, 462-470, 483-493, 589, 628-631, 642-650, 661-666, 705-728 |
| python/hsml/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsml/default\_predictor.py                                                |      672 |       56 |     92% |93-95, 116-117, 126-128, 161-169, 209-210, 226, 354-355, 385, 391, 397-406, 411-412, 491, 650-652, 704, 746, 777, 831, 1026-1027, 1043-1045, 1098, 1191-1193, 1511, 1513, 1516-1517, 1529, 1534-1535, 1548, 1552 |
| python/hsml/deployable\_component.py                                             |       56 |        5 |     91% |73, 95, 105, 115, 125 |
| python/hsml/deployable\_component\_logs.py                                       |       85 |        7 |     92% |114, 184, 189, 193, 198, 202, 205 |
| python/hsml/deployment.py                                                        |      489 |       71 |     85% |489, 496, 520-530, 537, 557-562, 594, 601, 610, 760-768, 868, 932, 1060, 1090, 1111, 1117, 1127, 1165, 1191, 1197, 1209, 1215, 1219, 1229, 1239, 1243, 1249, 1259, 1265, 1269, 1275, 1279, 1285, 1289, 1299, 1303, 1309, 1313, 1319, 1323, 1329, 1333, 1339, 1343, 1362, 1366, 1372, 1376, 1382, 1388, 1394, 1398, 1411, 1415, 1432, 1436, 1472, 1476, 1479-1484 |
| python/hsml/deployment\_logging\_config.py                                       |      171 |       16 |     91% |124, 139, 144-150, 200, 235, 239, 263, 269, 273, 283, 293, 303, 313, 323 |
| python/hsml/deployment\_schema.py                                                |      881 |       44 |     95% |95, 113, 116, 237, 345-348, 361, 639-640, 733, 810-811, 876, 893, 915, 917, 948, 954, 956, 960, 1035, 1038, 1157, 1202, 1232, 1248, 1257, 1260, 1273, 1277, 1286, 1296, 1365, 1372-1374, 1380, 1498, 1588, 1710, 1777, 1889 |
| python/hsml/deployment\_tracing\_config.py                                       |       93 |       11 |     88% |66, 71, 124-126, 129, 153, 163, 177, 191, 196 |
| python/hsml/deployment\_version.py                                               |      198 |       20 |     90% |213, 231, 249, 255, 261, 279, 324, 330, 336, 342, 354, 360, 366, 372, 378, 384, 390, 396, 402, 405 |
| python/hsml/engine/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/engine/local\_engine.py                                              |       53 |       33 |     38% |36-42, 45-46, 49, 58-76, 94-113, 116-118, 121-123, 126, 129-132 |
| python/hsml/engine/model\_engine.py                                              |      408 |      232 |     43% |82-102, 108-135, 145, 157-158, 168, 174-185, 203-232, 238-245, 258-278, 319-403, 406, 416-539, 592-594, 612, 617, 663-664, 669, 681, 684-685, 717-724, 735-738, 740, 746, 753-754, 770-771, 781-782, 814-816, 840-865, 868-885, 888-902, 905, 914, 924, 933, 942, 950, 967, 984 |
| python/hsml/engine/serving\_engine.py                                            |      714 |      223 |     69% |51-52, 105, 114, 123, 131, 143, 145, 150-167, 184, 201, 218-220, 224-229, 232-270, 273-312, 315-335, 338-369, 382-387, 390-391, 402-431, 437-444, 447-502, 524, 526, 528, 576-603, 631, 655, 678, 889, 896, 907-914, 917-944, 1138, 1298, 1308-1310, 1430, 1513, 1538, 1542, 1551, 1556-1591, 1605, 1610, 1618, 1624, 1685-1700 |
| python/hsml/inference\_batcher.py                                                |       77 |       10 |     87% |52, 83-85, 88, 108, 118, 128, 138, 141 |
| python/hsml/inference\_endpoint.py                                               |       84 |        7 |     92% |54, 67, 109-111, 133, 155 |
| python/hsml/inference\_logger.py                                                 |       66 |        8 |     88% |51, 93-95, 98, 114, 124, 127 |
| python/hsml/kafka\_topic/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/llm/\_\_init\_\_.py                                                  |        0 |        0 |    100% |           |
| python/hsml/llm/model.py                                                         |        8 |        2 |     75% |     72-73 |
| python/hsml/llm/predictor.py                                                     |       14 |        0 |    100% |           |
| python/hsml/llm/signature.py                                                     |       13 |        4 |     69% |     79-94 |
| python/hsml/model.py                                                             |      357 |       53 |     85% |144-149, 155-170, 302, 343, 485, 529, 562-563, 637, 668, 683, 712-715, 777-785, 796, 831, 862, 872, 882, 892, 902, 912, 922, 926, 936, 946, 950, 956, 960, 972, 982, 994, 1004, 1014, 1023, 1032, 1041, 1047, 1051, 1059 |
| python/hsml/model\_registry.py                                                   |      192 |       25 |     87% |79-80, 100-105, 133, 160-169, 175, 181, 187, 316, 321, 333, 360, 412, 427, 439, 445, 451, 457, 463, 466-471 |
| python/hsml/model\_schema.py                                                     |       21 |        5 |     76% |60, 67, 70-76 |
| python/hsml/model\_serving.py                                                    |      276 |       34 |     88% |40-41, 108, 136, 180-184, 187-195, 204, 243, 428, 514, 698, 750, 894, 900, 906, 909, 1023, 1046, 1053, 1063, 1068-1069, 1096, 1098, 1100, 1102, 1104 |
| python/hsml/predictor.py                                                         |      759 |       71 |     91% |67, 136, 231, 408, 451, 459-461, 579, 704, 714, 724, 734-735, 745, 749, 784, 794, 808, 818, 828, 850, 862, 872, 882, 892, 930-933, 970, 1006, 1026, 1042, 1046, 1056, 1066, 1186, 1196-1210, 1234-1239, 1261, 1291, 1325-1327, 1349, 1381, 1383, 1387, 1410-1411, 1421, 1442-1443, 1473, 1478-1479, 1511, 1520 |
| python/hsml/predictor\_state.py                                                  |       87 |       14 |     84% |53, 83-103, 160 |
| python/hsml/predictor\_state\_condition.py                                       |       50 |        7 |     86% |42, 62-64, 67, 70, 97 |
| python/hsml/python/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/python/endpoint.py                                                   |       10 |        0 |    100% |           |
| python/hsml/python/feature\_view\_endpoint.py                                    |        9 |        1 |     89% |        32 |
| python/hsml/python/model.py                                                      |        8 |        0 |    100% |           |
| python/hsml/python/predictor.py                                                  |        9 |        5 |     44% |     25-33 |
| python/hsml/python/signature.py                                                  |       13 |        0 |    100% |           |
| python/hsml/resources.py                                                         |      153 |       13 |     92% |51, 71, 84, 94, 104, 107, 149, 168, 203, 207, 237, 247, 250 |
| python/hsml/scaling\_config.py                                                   |      247 |       26 |     89% |93, 159, 294-296, 300, 329, 344-353, 370, 385, 399, 409, 419, 429, 439, 457, 485, 499, 527, 542 |
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
| python/hsml/util/\_\_init\_\_.py                                                 |       18 |       18 |      0% |      5-22 |
| python/hsml/utils/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/utils/local\_paths.py                                                |       50 |        1 |     98% |       133 |
| python/hsml/utils/schema/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hsml/utils/schema/column.py                                               |        7 |        0 |    100% |           |
| python/hsml/utils/schema/columnar\_schema.py                                     |       61 |        0 |    100% |           |
| python/hsml/utils/schema/tensor.py                                               |        8 |        0 |    100% |           |
| python/hsml/utils/schema/tensor\_schema.py                                       |       34 |        0 |    100% |           |
| python/hsml/version.py                                                           |        2 |        2 |      0% |     17-22 |
| **TOTAL**                                                                        | **50273** | **12933** | **74%** |           |


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