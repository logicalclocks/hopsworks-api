# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                             |    Stmts |     Miss |   Cover |   Missing |
|--------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| python/hopsworks/\_\_init\_\_.py                                                 |      243 |       82 |     66% |62, 77-79, 83-85, 95-97, 149, 152, 155, 164-166, 277, 305, 309, 317, 342, 368-370, 397-402, 416, 446, 452, 462, 464-496, 508, 539-541, 584-601, 612-614, 625-627, 631-633, 643, 653 |
| python/hopsworks/alert/\_\_init\_\_.py                                           |        6 |        6 |      0% |      5-10 |
| python/hopsworks/alert\_receiver/\_\_init\_\_.py                                 |        6 |        6 |      0% |      5-10 |
| python/hopsworks/app/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/cli/\_\_init\_\_.py                                             |        0 |        0 |    100% |           |
| python/hopsworks/cli/\_\_main\_\_.py                                             |        3 |        3 |      0% |       3-7 |
| python/hopsworks/cli/auth.py                                                     |       39 |        5 |     87% |38, 101, 103, 121, 123 |
| python/hopsworks/cli/commands/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks/cli/commands/agent.py                                           |      162 |       37 |     77% |39-40, 70-85, 143-144, 170-171, 196-197, 230-231, 236-237, 240-241, 309-321, 331-332, 334-335, 355, 358-359, 372-373, 406 |
| python/hopsworks/cli/commands/alert.py                                           |      321 |      117 |     64% |68-69, 107-108, 125-126, 129-135, 175-176, 178-179, 202-203, 228-250, 297-312, 340-341, 344-345, 347-348, 363-370, 436-437, 454-455, 457-458, 483-484, 502-509, 537-538, 540-541, 567-575, 607-608, 612-613, 639-647, 675-688, 701-702, 710-715, 720-726, 735, 739, 741 |
| python/hopsworks/cli/commands/app.py                                             |      288 |       32 |     89% |42-43, 123, 166, 171-172, 224-225, 356, 361, 371, 378, 380, 415-416, 480-481, 511-512, 526-527, 553, 560-561, 578-579, 604, 615, 622-623, 627, 642 |
| python/hopsworks/cli/commands/context.py                                         |       88 |       24 |     73% |112, 124-131, 134-137, 140-145, 148-155, 187, 231-232 |
| python/hopsworks/cli/commands/datasource.py                                      |      231 |       73 |     68% |62-63, 67-70, 82-97, 101, 194, 196, 198, 252, 289, 291, 361-378, 395, 406-407, 425-426, 428-429, 445-459, 472-482, 513-514, 527-528, 531-532, 544, 559-560 |
| python/hopsworks/cli/commands/deployment.py                                      |      212 |       64 |     70% |36-37, 68-69, 73-86, 99-104, 124, 149, 215, 219-220, 222, 228-243, 253-254, 285-286, 311-312, 341, 345-346, 351-352, 355-356, 428-444, 454-455, 457-458, 478, 481-482, 524-525, 527 |
| python/hopsworks/cli/commands/env.py                                             |       55 |       33 |     40% |26, 30, 45-50, 93-113, 146-168 |
| python/hopsworks/cli/commands/fg.py                                              |      449 |      163 |     64% |137-138, 150-151, 257-259, 290, 292, 382-414, 504-553, 603-612, 618-619, 623, 625, 668-707, 742, 745-746, 789, 791, 804-805, 831-832, 836-862, 906-907, 910-913, 935-936, 939-940, 966-967, 990-991, 1000-1006, 1021-1022, 1032-1034, 1047, 1054, 1078-1098, 1120, 1129-1137, 1144, 1148-1151 |
| python/hopsworks/cli/commands/files.py                                           |      103 |       31 |     70% |43-44, 77-78, 115-116, 141-148, 181-190, 214-221, 237, 241-242 |
| python/hopsworks/cli/commands/fv.py                                              |      233 |       85 |     64% |67-68, 86-96, 143-144, 170-186, 282-283, 289-290, 293-294, 318-319, 349-350, 355-356, 361, 399-425, 453-454, 457, 463-464, 474-478, 488, 499, 508-528, 532-540 |
| python/hopsworks/cli/commands/init.py                                            |       66 |        3 |     95% |59, 98, 143 |
| python/hopsworks/cli/commands/job.py                                             |      327 |      149 |     54% |32, 35-42, 52, 54-58, 78-79, 108-109, 113-124, 128-141, 184-197, 266-318, 370, 379-380, 389, 438-459, 472-480, 527, 530-533, 540-541, 545-546, 555-556, 559-560, 711-712, 715, 728-742, 758-759, 775-782, 788, 792, 800-801, 803, 810-811, 817, 826-829 |
| python/hopsworks/cli/commands/login.py                                           |       27 |       17 |     37% |     53-84 |
| python/hopsworks/cli/commands/model.py                                           |      163 |       54 |     67% |66-69, 72, 91-107, 112, 207, 212-215, 219-223, 238-239, 272-273, 291-298, 339-346, 351, 356, 360-361 |
| python/hopsworks/cli/commands/project.py                                         |       52 |       36 |     31% |22-43, 56-74, 85-96, 100 |
| python/hopsworks/cli/commands/search.py                                          |       56 |       38 |     32% |78-126, 135-141, 150-160 |
| python/hopsworks/cli/commands/setup.py                                           |      159 |       53 |     67% |53-60, 81, 85-91, 113, 151-169, 204, 305, 315-321, 333-334, 340-341, 354, 376-377, 411-412, 427-433 |
| python/hopsworks/cli/commands/skills.py                                          |       95 |       33 |     65% |31-32, 39-41, 47, 66-67, 69, 141-154, 168-190 |
| python/hopsworks/cli/commands/superset.py                                        |      138 |       38 |     72% |51-52, 91-101, 116-122, 141-142, 155, 199-200, 217, 220-221, 255, 278-279, 295-301, 328-330, 336-337, 345 |
| python/hopsworks/cli/commands/td.py                                              |      147 |       55 |     63% |53-54, 60-61, 132-133, 141, 184-212, 239-253, 320-321, 332, 335-337, 348, 352-353, 360-368 |
| python/hopsworks/cli/commands/transformation.py                                  |       84 |       13 |     85% |36-37, 98, 103-104, 115-116, 173, 180-181, 184, 191, 194 |
| python/hopsworks/cli/commands/trino.py                                           |      134 |       80 |     40% |45, 64-65, 70-71, 76-85, 90-95, 108-110, 114-116, 127-154, 220-231, 251, 264, 279-285, 300-305, 319-330 |
| python/hopsworks/cli/commands/update.py                                          |       49 |       15 |     69% |54, 69-70, 73, 76, 87-96, 107-108 |
| python/hopsworks/cli/config.py                                                   |      155 |       22 |     86% |75, 89, 105, 109-110, 119-121, 140, 144, 149-151, 168-169, 245, 247, 331-337 |
| python/hopsworks/cli/joinspec.py                                                 |       18 |        0 |    100% |           |
| python/hopsworks/cli/lineage.py                                                  |       33 |        4 |     88% | 55, 62-64 |
| python/hopsworks/cli/main.py                                                     |       67 |        7 |     90% |59-60, 168, 242-244, 256 |
| python/hopsworks/cli/output.py                                                   |       68 |        5 |     93% |73, 159, 171, 183, 194 |
| python/hopsworks/cli/session.py                                                  |       52 |        8 |     85% |72, 90-91, 107, 109-111, 129 |
| python/hopsworks/cli/templates/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/client/\_\_init\_\_.py                                          |       15 |        0 |    100% |           |
| python/hopsworks/client/auth/\_\_init\_\_.py                                     |        4 |        4 |      0% |       5-8 |
| python/hopsworks/client/base/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/exceptions/\_\_init\_\_.py                               |       21 |        0 |    100% |           |
| python/hopsworks/client/external/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/hopsworks/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/command/\_\_init\_\_.py                                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/connection/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/constants.py                                                    |        2 |        2 |      0% |     17-45 |
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
| python/hopsworks/core/library\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/opensearch/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/opensearch\_api/\_\_init\_\_.py                            |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/project\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/rest\_endpoint/\_\_init\_\_.py                             |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/search\_api/\_\_init\_\_.py                                |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/secret\_api/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/services\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/superset\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tag\_schemas\_api/\_\_init\_\_.py                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tags\_api/\_\_init\_\_.py                                  |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/trino\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
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
| python/hopsworks/secret/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/spark.py                                                        |       17 |       17 |      0% |     18-95 |
| python/hopsworks/tag/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/triggered\_alert/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/user/\_\_init\_\_.py                                            |        2 |        2 |      0% |       5-6 |
| python/hopsworks/util/\_\_init\_\_.py                                            |       32 |       32 |      0% |      5-36 |
| python/hopsworks/version.py                                                      |        2 |        2 |      0% |     17-22 |
| python/hopsworks\_common/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hopsworks\_common/alert.py                                                |      160 |       48 |     70% |97-109, 123, 135, 141, 147, 153, 156, 164, 173, 176, 196-207, 213, 219, 225, 229, 241, 260-270, 276, 282, 286, 297, 317-328, 334, 340, 346, 350, 362, 383-395, 401, 407, 413, 419, 423, 436 |
| python/hopsworks\_common/alert\_receiver.py                                      |      212 |       94 |     56% |32-37, 40, 48, 69, 74, 77, 81, 87, 90, 100-105, 108, 116, 137, 142, 145, 149, 155, 158, 169-177, 180, 184, 199-201, 205-208, 213, 218, 221, 225, 232, 235, 245-250, 253, 257, 278, 283, 286, 290, 296, 299, 346-347, 359, 365, 371, 377, 380, 384-399, 402, 405-413, 416 |
| python/hopsworks\_common/alert\_route.py                                         |       50 |       12 |     76% |50-51, 56, 61, 66, 71, 76, 86, 89, 93, 104, 107 |
| python/hopsworks\_common/app.py                                                  |      285 |       14 |     95% |130, 139, 177, 247, 253, 334, 419-422, 466-467, 529, 535 |
| python/hopsworks\_common/client/\_\_init\_\_.py                                  |       67 |       20 |     70% |43-59, 67, 74, 77, 83, 92, 98, 107, 113, 120, 129, 135, 149, 157 |
| python/hopsworks\_common/client/auth.py                                          |       36 |       14 |     61% |39-40, 52, 55-56, 71-72, 77-83 |
| python/hopsworks\_common/client/base.py                                          |      211 |       55 |     74% |73-78, 86-91, 95, 99, 103-104, 115, 118-119, 158, 186, 190, 215, 344, 349, 355, 376, 383-395, 400-402, 410-415, 419-425, 429-437 |
| python/hopsworks\_common/client/exceptions.py                                    |      148 |        8 |     95% |48-50, 56, 155-159, 167, 179 |
| python/hopsworks\_common/client/external.py                                      |      205 |       86 |     58% |63-106, 109-203, 265-288, 386-387, 395-399, 426, 429, 433, 437 |
| python/hopsworks\_common/client/hopsworks.py                                     |      102 |       63 |     38% |56-82, 86-92, 96, 99, 102, 105, 113-118, 126-131, 134-142, 145-149, 157-164, 175, 178, 182 |
| python/hopsworks\_common/client/istio/\_\_init\_\_.py                            |       12 |        6 |     50% | 29-34, 39 |
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
| python/hopsworks\_common/client/istio/utils/infer\_type.py                       |      330 |      253 |     23% |55, 75-105, 121-123, 126-129, 140, 151, 159, 190-197, 202, 207, 212, 217, 222, 231, 234-241, 259-316, 320-336, 366-375, 379-389, 404-417, 425-452, 465-474, 500-507, 518, 529, 534, 545, 556, 564, 572-579, 595-652, 683-692, 696-706, 717-727, 740-755, 763-790 |
| python/hopsworks\_common/client/istio/utils/numpy\_codec.py                      |       35 |       29 |     17% |24-39, 44-70 |
| python/hopsworks\_common/client/online\_store\_rest\_client.py                   |      202 |       76 |     62% |52-58, 69-73, 75, 107, 111, 132-144, 159, 161, 165, 168, 174, 182, 185, 190-193, 196-200, 203, 229, 238, 253, 258-261, 283, 291, 296, 301, 307, 319-331, 339, 346, 355, 361, 371, 376-377, 388-401, 406, 414, 419, 427 |
| python/hopsworks\_common/command.py                                              |       25 |       14 |     44% |38-45, 49-52, 56, 60 |
| python/hopsworks\_common/connection.py                                           |      242 |      100 |     59% |178-180, 224, 256, 271-282, 292, 305, 314-329, 348-421, 470-481, 559, 573, 578, 582, 587, 591, 596, 600, 605, 609, 614, 623, 627, 631, 636, 645-648, 653, 658, 661-662, 665 |
| python/hopsworks\_common/constants.py                                            |      183 |        2 |     99% |    25, 28 |
| python/hopsworks\_common/core/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks\_common/core/alerts\_api.py                                     |      270 |      170 |     37% |178-181, 213-216, 245-248, 281-291, 325-336, 375-387, 423-436, 477-491, 540-574, 624-662, 712-737, 778-802, 830-834, 868-873, 916-981, 1005-1009, 1052-1084, 1117-1121, 1133-1136, 1144-1189, 1201-1205, 1208-1221 |
| python/hopsworks\_common/core/app\_api.py                                        |      148 |       38 |     74% |52-58, 75-89, 172, 175, 185, 192, 194, 254-269, 273-284, 338-345, 351 |
| python/hopsworks\_common/core/constants.py                                       |       32 |        0 |    100% |           |
| python/hopsworks\_common/core/dataset.py                                         |       31 |       14 |     55% |33-37, 41-44, 48, 52, 56, 60, 64 |
| python/hopsworks\_common/core/dataset\_api.py                                    |      365 |      241 |     34% |106-168, 217-288, 300-366, 388, 405-408, 411, 422-426, 456, 471-472, 486, 499-501, 515, 559-583, 614-632, 658-667, 695-713, 739-757, 775-798, 830-849, 870-887, 903-918, 934-938, 967-1019, 1038, 1062, 1086-1098, 1112-1122, 1148-1149 |
| python/hopsworks\_common/core/env\_var\_api.py                                   |       59 |        1 |     98% |       254 |
| python/hopsworks\_common/core/environment\_api.py                                |       42 |       22 |     48% |63-86, 109-114, 147-152, 164-174 |
| python/hopsworks\_common/core/execution\_api.py                                  |       69 |       18 |     74% |66, 90-101, 106-112, 120-129, 132-142, 208-210 |
| python/hopsworks\_common/core/execution\_pod\_log.py                             |       41 |        2 |     95% |   81, 102 |
| python/hopsworks\_common/core/git\_api.py                                        |      177 |      130 |     27% |87-122, 135-142, 157, 176, 208-211, 228-254, 257-265, 268-291, 294-316, 321-344, 347-380, 383-410, 413-440, 443-470, 473-499, 502-529, 532-556, 559-572, 578-584 |
| python/hopsworks\_common/core/git\_op\_execution\_api.py                         |        9 |        4 |     56% |     24-36 |
| python/hopsworks\_common/core/git\_provider\_api.py                              |       45 |       31 |     31% |31-34, 39-44, 49-67, 70-81, 88-96 |
| python/hopsworks\_common/core/git\_remote\_api.py                                |       35 |       25 |     29% |25, 28-43, 46-61, 64-88, 91-113 |
| python/hopsworks\_common/core/hosts\_api.py                                      |       10 |        3 |     70% |     28-32 |
| python/hopsworks\_common/core/ingestion\_job.py                                  |       19 |        0 |    100% |           |
| python/hopsworks\_common/core/ingestion\_job\_conf.py                            |       39 |       14 |     64% |33-36, 40, 44, 48, 52, 56, 60, 64, 68, 71, 74 |
| python/hopsworks\_common/core/inode.py                                           |       41 |        5 |     88% |51, 59, 67, 71, 75 |
| python/hopsworks\_common/core/job\_api.py                                        |      108 |       49 |     55% |78-91, 108-116, 131-138, 156-157, 176-186, 194-201, 213-220, 228-233, 240-243, 259-263, 292-298, 329-332 |
| python/hopsworks\_common/core/job\_configuration.py                              |       21 |        1 |     95% |        75 |
| python/hopsworks\_common/core/kafka\_api.py                                      |       99 |       56 |     43% |70-82, 128-149, 165-170, 183-186, 196-204, 213-223, 236-243, 259-275, 292-296, 305-316, 321-330, 340, 369-389 |
| python/hopsworks\_common/core/library\_api.py                                    |       15 |        4 |     73% |     41-54 |
| python/hopsworks\_common/core/opensearch.py                                      |      239 |       75 |     69% |44, 52-98, 126, 161, 194, 205-208, 212-218, 223-230, 294-296, 305, 382-417, 460-463, 468-480, 521 |
| python/hopsworks\_common/core/opensearch\_api.py                                 |       54 |       15 |     72% |62-73, 86-87, 113-114, 135-140 |
| python/hopsworks\_common/core/project\_api.py                                    |       58 |       32 |     45% |39-43, 54-64, 75-79, 90-94, 108-115, 167-173, 176-179 |
| python/hopsworks\_common/core/rest.py                                            |       18 |        1 |     94% |        63 |
| python/hopsworks\_common/core/rest\_endpoint.py                                  |      236 |       59 |     75% |48-51, 60, 63, 68, 102, 106-109, 115, 124, 130, 141, 144-146, 152, 157, 167, 182-183, 186, 201-203, 206, 271-277, 280, 305-310, 313, 325, 333, 366, 382, 385, 410-413, 418, 429-433, 440, 450 |
| python/hopsworks\_common/core/search\_api.py                                     |      133 |       82 |     38% |45-47, 53, 59, 65, 69, 72, 84-103, 110, 115, 119, 122, 134-138, 237, 299-308, 362-371, 425-434, 485-494, 510-525, 543-562, 576-624 |
| python/hopsworks\_common/core/secret\_api.py                                     |       61 |       16 |     74% |57-62, 92-93, 119-125, 161-163, 231-237 |
| python/hopsworks\_common/core/services\_api.py                                   |       10 |        3 |     70% |     30-35 |
| python/hopsworks\_common/core/sink\_job\_configuration.py                        |      379 |       57 |     85% |67, 95-96, 99, 106-107, 115, 119, 124, 128, 152, 186-193, 195, 203, 249, 287, 296, 385, 389, 394, 398, 404-416, 425-433, 438, 507, 518, 527, 536, 545, 554, 563, 587, 596, 605, 615, 627, 639, 768 |
| python/hopsworks\_common/core/superset\_api.py                                   |      156 |       45 |     71% |110-111, 210, 240-252, 265, 275, 290, 303, 337-350, 363, 373, 388, 401, 433-447, 460, 470, 485, 500 |
| python/hopsworks\_common/core/tag\_schemas\_api.py                               |       50 |       50 |      0% |    23-179 |
| python/hopsworks\_common/core/tags\_api.py                                       |       39 |        9 |     77% |62-68, 86-91, 143 |
| python/hopsworks\_common/core/trino\_api.py                                      |       95 |        1 |     99% |       225 |
| python/hopsworks\_common/core/type\_systems.py                                   |      258 |       43 |     83% |319, 372, 394-396, 402-424, 464, 485, 489-513, 520 |
| python/hopsworks\_common/core/variable\_api.py                                   |       50 |       26 |     48% |74-82, 96-101, 112, 150-159, 169-170, 178-182 |
| python/hopsworks\_common/decorators.py                                           |       94 |        5 |     95% |131, 157, 167, 177, 187 |
| python/hopsworks\_common/engine/\_\_init\_\_.py                                  |        0 |        0 |    100% |           |
| python/hopsworks\_common/engine/alerts\_engine.py                                |       64 |        2 |     97% |   45, 101 |
| python/hopsworks\_common/engine/environment\_engine.py                           |       38 |       27 |     29% |27-31, 34-38, 41-45, 48-74, 77-90 |
| python/hopsworks\_common/engine/execution\_engine.py                             |      108 |       62 |     43% |57-80, 83-102, 119-144, 168, 179-193, 203-209, 216, 236 |
| python/hopsworks\_common/engine/git\_engine.py                                   |       21 |        8 |     62% |     49-62 |
| python/hopsworks\_common/env\_var.py                                             |       75 |        6 |     92% |106, 112, 120, 147, 150, 158 |
| python/hopsworks\_common/environment.py                                          |       75 |       24 |     68% |63-66, 72, 84, 115-131, 162-178, 224, 227 |
| python/hopsworks\_common/execution.py                                            |      153 |       23 |     85% |73, 79-81, 123, 141, 147, 153, 159, 165, 171, 185, 190, 210, 239, 252, 265, 330, 333, 336, 341-349 |
| python/hopsworks\_common/git\_commit.py                                          |       53 |       26 |     51% |40-49, 53-60, 66, 72, 78, 84, 90, 93, 96, 99 |
| python/hopsworks\_common/git\_file\_status.py                                    |       37 |       15 |     59% |34-36, 40-45, 51, 69, 75, 78, 81, 84 |
| python/hopsworks\_common/git\_op\_execution.py                                   |       54 |       25 |     54% |44-52, 56-57, 62, 67, 72, 77, 82, 87, 92, 97, 102, 107-114 |
| python/hopsworks\_common/git\_provider.py                                        |       40 |       15 |     62% |42-45, 50-53, 59, 65, 71, 81, 84, 87, 90 |
| python/hopsworks\_common/git\_remote.py                                          |       37 |       15 |     59% |40-43, 47-52, 58, 64, 74, 77, 80, 83 |
| python/hopsworks\_common/git\_repo.py                                            |      129 |       50 |     61% |52-68, 72-77, 83, 89, 95, 101, 107, 113, 119, 125, 137, 150, 164-167, 179, 192, 205, 220, 234, 248, 262, 277, 292, 321, 336, 348, 351, 354, 357 |
| python/hopsworks\_common/job.py                                                  |      198 |       59 |     70% |81-88, 113, 119, 123, 135, 141, 159, 250, 305-309, 329-333, 345, 362, 375, 381-398, 409, 482-498, 504-505, 511-521, 527-537, 550, 566, 599, 602-605, 608, 611, 614, 619-621 |
| python/hopsworks\_common/job\_schedule.py                                        |       90 |       11 |     88% |112, 115, 118, 136, 154, 160, 166, 176, 182, 188, 194 |
| python/hopsworks\_common/kafka\_schema.py                                        |       51 |       22 |     57% |36-41, 45-50, 53-55, 61, 67, 73, 79, 92, 95, 98, 101 |
| python/hopsworks\_common/kafka\_topic.py                                         |      106 |       17 |     84% |70, 97-99, 118-120, 130, 136, 146, 152, 162, 168, 183, 186, 198, 201 |
| python/hopsworks\_common/library.py                                              |       19 |       11 |     42% |39-50, 54-55 |
| python/hopsworks\_common/project.py                                              |      145 |       43 |     70% |98-101, 107, 113, 119, 125, 131, 137, 232-235, 244-247, 256, 274, 279, 288, 297, 306, 315, 324, 333-335, 344-346, 358, 370, 400, 443, 448, 451, 454-456, 461-462 |
| python/hopsworks\_common/search\_results.py                                      |      224 |       40 |     82% |58, 61, 110, 116, 122, 146, 156-172, 199, 217, 235, 243, 253-262, 398, 404, 410, 416, 448, 464 |
| python/hopsworks\_common/secret.py                                               |       61 |       11 |     82% |61, 80, 86, 92, 98, 110, 113, 116, 119-121 |
| python/hopsworks\_common/spark\_connect\_utils.py                                |       38 |        6 |     84% |66-67, 103-104, 110-111 |
| python/hopsworks\_common/tag.py                                                  |       80 |        6 |     92% |69, 120, 141, 153, 156, 159 |
| python/hopsworks\_common/triggered\_alert.py                                     |       87 |       39 |     55% |26-28, 32-35, 38, 41, 48, 51, 71-79, 83-88, 94, 100, 106, 112, 118, 124, 130, 136, 142, 145, 148, 161, 164 |
| python/hopsworks\_common/usage.py                                                |      179 |      105 |     41% |48-52, 55-57, 60-62, 65-67, 70-72, 75-85, 88-90, 93, 96, 99-101, 104, 126-127, 130-131, 134-141, 144, 170, 175, 179, 184-186, 190-191, 195-199, 203-205, 209-212, 216-224, 232-257, 261-288, 292-296 |
| python/hopsworks\_common/user.py                                                 |       42 |        5 |     88% |54-55, 60, 65, 68 |
| python/hopsworks\_common/util.py                                                 |      548 |      141 |     74% |67-68, 80-102, 105-108, 168-171, 280, 333, 338, 449, 491-518, 523, 566, 570-573, 579-595, 601-607, 612-614, 634, 662, 688, 800, 806-808, 816-821, 866, 877-888, 944-949, 954, 958, 963, 967, 972, 1000-1008, 1012-1040, 1044-1046, 1050-1067, 1079-1086, 1091, 1096, 1101 |
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
| python/hsfs/constructor/fs\_query.py                                             |       69 |       19 |     72% |71, 79, 105, 111, 117, 121, 125, 131-137, 148-149, 160-161, 175-176 |
| python/hsfs/constructor/hudi\_feature\_group\_alias.py                           |       26 |        0 |    100% |           |
| python/hsfs/constructor/join.py                                                  |       39 |        2 |     95% |    56, 84 |
| python/hsfs/constructor/lookback.py                                              |      147 |       13 |     91% |82, 172, 174, 176, 181, 321, 323, 348, 350, 357, 360, 363, 366 |
| python/hsfs/constructor/partitioned\_by\_translator.py                           |      140 |       23 |     84% |106, 130, 132, 134, 186, 190-191, 195-196, 215, 217, 221-222, 246, 248, 284-288, 294, 301, 306 |
| python/hsfs/constructor/prepared\_statement\_parameter.py                        |       35 |        8 |     77% |46-48, 51, 54, 57, 69, 73 |
| python/hsfs/constructor/query.py                                                 |      368 |       70 |     81% |112-114, 331, 336, 343, 373-375, 403-408, 438, 637-643, 706-711, 735, 751, 769, 801-819, 837-841, 864-866, 872, 875, 877, 880, 883-889, 926, 930, 1011, 1039-1042, 1146-1153, 1188-1189, 1191, 1200-1201, 1212 |
| python/hsfs/constructor/serving\_prepared\_statement.py                          |       65 |       19 |     71% |61-63, 66, 69, 76-82, 86, 90, 96, 100, 104, 108, 112, 136, 140 |
| python/hsfs/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsfs/core/arrow\_flight\_client.py                                        |      316 |      156 |     51% |30, 71-75, 79, 83, 89, 95, 103, 109, 116, 161, 171, 213-214, 229-237, 244-263, 271-277, 282-296, 303, 308, 343-347, 355-362, 372-375, 381-388, 391-394, 397-400, 403-405, 408-410, 418-427, 433-455, 468-470, 483-499, 504-506, 529-530, 563-603, 611, 615, 620, 624, 638, 643 |
| python/hsfs/core/chart.py                                                        |      101 |       39 |     61% |47-55, 59-66, 69, 82, 87, 91, 96, 100, 105, 109, 114, 118, 123, 127, 132, 136, 141, 145, 150, 154, 159, 163, 172-174, 185-187 |
| python/hsfs/core/chart\_api.py                                                   |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/constants.py                                                    |        2 |        0 |    100% |           |
| python/hsfs/core/dashboard.py                                                    |       53 |       21 |     60% |41-43, 47-54, 57, 64, 69, 73, 78, 82, 87, 91, 100-102, 113-115 |
| python/hsfs/core/dashboard\_api.py                                               |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/data\_source.py                                                 |      198 |       23 |     88% |154, 165, 202, 212, 255, 265, 294, 318, 343, 362, 398, 555, 604, 616, 632, 644, 649, 653, 658, 662, 676, 724, 726 |
| python/hsfs/core/data\_source\_api.py                                            |      108 |       64 |     41% |43-55, 60-71, 76-87, 94-120, 128-136, 144-158, 174-190, 200-214, 230-258, 264-278, 283-297, 368-380 |
| python/hsfs/core/data\_source\_data.py                                           |       48 |        6 |     88% |56-61, 67, 103 |
| python/hsfs/core/dataset\_api/\_\_init\_\_.py                                    |        2 |        0 |    100% |           |
| python/hsfs/core/delta\_engine.py                                                |      602 |      143 |     76% |58-62, 81-100, 138-139, 310, 326-355, 363, 374, 380-415, 471-472, 593-603, 636-639, 670-672, 706, 708-717, 719-728, 730-737, 815-816, 831-835, 838-843, 855, 950, 957, 1010-1025, 1042-1075, 1135, 1138, 1153, 1171-1183, 1215, 1235-1236, 1238-1240, 1328 |
| python/hsfs/core/deltastreamer\_jobconf.py                                       |       16 |        6 |     62% | 30, 37-44 |
| python/hsfs/core/distribution\_distance.py                                       |       54 |        2 |     96% |   165-166 |
| python/hsfs/core/distribution\_engine.py                                         |      196 |       25 |     87% |201, 207, 290, 296-302, 382-402, 442, 447, 452, 479, 482-483, 492, 498, 509 |
| python/hsfs/core/execution/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/expectation\_api.py                                             |       32 |       19 |     41% |46-61, 74-90, 100-114, 125-139, 149-162 |
| python/hsfs/core/expectation\_engine.py                                          |       25 |        2 |     92% |    61, 67 |
| python/hsfs/core/expectation\_suite\_api.py                                      |       53 |       39 |     26% |44-64, 77-100, 115-140, 150-168, 176-187 |
| python/hsfs/core/expectation\_suite\_engine.py                                   |       37 |        2 |     95% |    68, 82 |
| python/hsfs/core/explicit\_provenance.py                                         |      229 |      152 |     34% |35, 38, 41, 44, 57-63, 69, 75, 81, 84, 87, 98, 101, 109-123, 137-140, 149, 159, 175, 184, 188, 207, 210, 217-234, 238-244, 248-261, 265-282, 286-303, 307-352, 372-434, 439-486 |
| python/hsfs/core/external\_feature\_group\_engine.py                             |       57 |       18 |     68% |36, 79, 93-130, 203-205 |
| python/hsfs/core/feature\_descriptive\_statistics.py                             |      185 |       20 |     89% |107, 110-111, 127-128, 176, 215, 218, 221, 251, 257, 263, 269, 287, 299, 311, 327, 341, 351, 357 |
| python/hsfs/core/feature\_group\_api.py                                          |      146 |       92 |     37% |51-68, 82-99, 174-180, 188-190, 207-234, 250-270, 283-293, 312-326, 355-366, 390-401, 426-441, 459-471, 489-501, 524-541, 562-579, 603-620, 644-661 |
| python/hsfs/core/feature\_group\_base\_engine.py                                 |       77 |       19 |     75% |78, 92, 109-112, 129-132, 149-152, 169-172, 216 |
| python/hsfs/core/feature\_group\_engine.py                                       |      344 |       43 |     88% |135, 217-231, 266-267, 380, 392-399, 486-520, 781-783, 796-798, 811-812, 852-853, 927, 939, 978, 996, 1063-1070, 1119, 1177, 1184 |
| python/hsfs/core/feature\_logging.py                                             |       75 |       34 |     55% |29, 58-61, 65-81, 89-91, 96, 101, 106, 123-134, 139, 142, 150, 153 |
| python/hsfs/core/feature\_logging\_client.py                                     |       52 |       52 |      0% |    16-109 |
| python/hsfs/core/feature\_monitoring\_config.py                                  |      412 |       98 |     76% |57, 61, 65-67, 74-76, 79, 82, 93, 97, 100, 175-178, 234-236, 256, 261-266, 296, 302, 305, 308, 311, 404-414, 521, 524, 537, 555-561, 639, 642, 651-654, 673, 713, 752-765, 785, 811-816, 839-845, 866-871, 890, 911, 946-951, 982, 994, 998, 1008-1020, 1026, 1032, 1038, 1044, 1050, 1085, 1087, 1089, 1103, 1105, 1130-1131, 1145, 1159, 1165, 1179, 1189, 1217, 1222, 1232 |
| python/hsfs/core/feature\_monitoring\_config\_api.py                             |       77 |       50 |     35% |63-70, 86-95, 108-114, 128-134, 150-156, 176-181, 191-197, 213-219, 233-240, 256-266, 290-318 |
| python/hsfs/core/feature\_monitoring\_config\_engine.py                          |      243 |       47 |     81% |165, 213, 295-317, 334-339, 348, 377-401, 417-419, 433, 479, 482, 486, 540, 677-692, 746, 788, 832-835, 841 |
| python/hsfs/core/feature\_monitoring\_result.py                                  |      101 |       21 |     79% |108, 111-130, 133, 136, 139-142, 148, 154, 160, 166, 172, 190 |
| python/hsfs/core/feature\_monitoring\_result\_api.py                             |       47 |       27 |     43% |62-69, 82-88, 108-116, 132-141, 155-161, 177-196 |
| python/hsfs/core/feature\_monitoring\_result\_engine.py                          |      196 |       61 |     69% |99, 152-172, 191, 252-330, 338, 389, 439, 460, 462, 464, 500-501, 540, 545, 734, 749-763 |
| python/hsfs/core/feature\_statistics\_config.py                                  |       52 |       10 |     81% |76-78, 82-90, 93, 96, 99 |
| python/hsfs/core/feature\_statistics\_result.py                                  |      100 |       25 |     75% |111-113, 117-136, 139, 142, 145-148, 154, 196 |
| python/hsfs/core/feature\_store\_activity\_api.py                                |       12 |       12 |      0% |     16-73 |
| python/hsfs/core/feature\_store\_api.py                                          |       12 |        3 |     75% |     32-34 |
| python/hsfs/core/feature\_view\_api.py                                           |      169 |       97 |     43% |71-72, 82-83, 105-129, 146-169, 174-180, 187-189, 194-196, 242, 272-285, 295-297, 306-309, 316-317, 328-332, 339-340, 345-348, 351-352, 357-361, 366-375, 399-413, 440-454, 467-476, 483-491, 498-506, 513-528, 538-545, 555-567 |
| python/hsfs/core/feature\_view\_engine.py                                        |      582 |      173 |     70% |104-106, 154-162, 171-179, 189, 218-219, 287, 393, 401-407, 409-414, 416-423, 446-453, 456, 484, 696-714, 718, 722, 740-741, 889-893, 950, 966, 975, 1072, 1113, 1154, 1176-1182, 1185-1188, 1251-1265, 1291, 1293, 1323-1324, 1336, 1349-1354, 1379, 1403-1408, 1427-1434, 1442-1443, 1514-1517, 1551-1554, 1623, 1678-1694, 1697, 1700-1703, 1802-1897, 2157, 2161, 2179-2224, 2230-2234, 2237-2261, 2264-2270, 2279-2282, 2285, 2288, 2293-2307, 2310-2311 |
| python/hsfs/core/glue\_catalog.py                                                |      111 |       24 |     78% |80, 102, 116, 139, 147, 167, 192-204, 227, 274-278, 311, 313 |
| python/hsfs/core/great\_expectation\_engine.py                                   |       43 |        4 |     91% |75, 95-100 |
| python/hsfs/core/hosts\_api/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/hudi\_engine.py                                                 |      168 |       11 |     93% |282, 304-309, 327-331, 340-344 |
| python/hsfs/core/iceberg\_engine.py                                              |      851 |      187 |     78% |99, 117, 125, 140-141, 150, 163, 173, 177, 180, 280, 290-298, 360, 362, 365-366, 374, 384-401, 518-526, 637, 639, 663, 672, 698, 718-747, 769, 777-803, 814-844, 909-925, 936, 1005, 1014-1022, 1079, 1091, 1107-1108, 1117, 1160-1161, 1283-1285, 1419-1422, 1427-1440, 1450-1476, 1576-1595, 1615-1624, 1647, 1690, 1716-1718, 1727-1745, 1781-1782, 1791, 1942-1946, 1964-1973, 1975-1977 |
| python/hsfs/core/inferred\_metadata.py                                           |       65 |       10 |     85% |54-56, 80, 88, 96, 118, 138, 165, 172 |
| python/hsfs/core/ingestion\_job/\_\_init\_\_.py                                  |        2 |        0 |    100% |           |
| python/hsfs/core/ingestion\_job\_conf/\_\_init\_\_.py                            |        2 |        0 |    100% |           |
| python/hsfs/core/inode/\_\_init\_\_.py                                           |        2 |        0 |    100% |           |
| python/hsfs/core/job/\_\_init\_\_.py                                             |        2 |        0 |    100% |           |
| python/hsfs/core/job\_api/\_\_init\_\_.py                                        |        2 |        0 |    100% |           |
| python/hsfs/core/job\_configuration/\_\_init\_\_.py                              |        2 |        0 |    100% |           |
| python/hsfs/core/job\_schedule/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_api/\_\_init\_\_.py                                      |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_engine.py                                                |      143 |       12 |     92% |244, 260, 266, 281, 321-332 |
| python/hsfs/core/monitoring\_window\_config.py                                   |      130 |       29 |     78% |49, 53-55, 63, 67, 70, 152-153, 168-169, 174, 177, 180, 186, 197, 203-214, 231, 236-240, 257, 280, 289 |
| python/hsfs/core/monitoring\_window\_config\_engine.py                           |      189 |       46 |     76% |49, 69, 78, 123, 160, 224, 237-245, 258, 335-382, 501-512, 543-557, 653, 696, 714 |
| python/hsfs/core/multi\_table\_ingestion.py                                      |      152 |        7 |     95% |72, 258-259, 268, 315, 331, 407 |
| python/hsfs/core/online\_ingestion.py                                            |       84 |       35 |     58% |97, 102, 105-110, 115-116, 124, 132, 144, 153, 161, 167, 181-219, 229-256 |
| python/hsfs/core/online\_ingestion\_api.py                                       |       14 |        7 |     50% |52-64, 93-104 |
| python/hsfs/core/online\_ingestion\_result.py                                    |       40 |       18 |     55% |48-50, 64-75, 83, 95, 101, 107, 113 |
| python/hsfs/core/online\_store\_rest\_client\_api.py                             |       58 |       31 |     47% |38-53, 97-101, 142-146, 161-168, 187-192 |
| python/hsfs/core/online\_store\_rest\_client\_engine.py                          |      162 |       46 |     72% |60, 85, 90, 112, 140, 161, 175-186, 230-256, 298-301, 313, 317, 328, 374, 385, 392, 396, 404-424, 429, 433, 447, 464, 475, 479, 483, 487, 491, 509 |
| python/hsfs/core/online\_store\_sql\_engine.py                                   |      379 |      286 |     25% |79-103, 108-109, 128-179, 192-214, 224-254, 268-289, 292-319, 342-348, 374-380, 394, 409, 417-477, 485-595, 598-609, 612, 615-626, 641-643, 653-660, 666-682, 690-712, 715, 727-732, 742-757, 767-805, 809, 814, 818, 829, 836, 843, 850, 855, 859-861, 868, 873, 882, 886-903, 907, 911, 915, 919, 923, 927 |
| python/hsfs/core/opensearch/\_\_init\_\_.py                                      |        3 |        0 |    100% |           |
| python/hsfs/core/opensearch\_api/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hsfs/core/partition\_grains.py                                            |       26 |        1 |     96% |        67 |
| python/hsfs/core/partition\_transforms.py                                        |      163 |        7 |     96% |261, 405, 410, 435-438 |
| python/hsfs/core/project\_api/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/query\_constructor\_api.py                                      |       10 |        5 |     50% |     24-32 |
| python/hsfs/core/schema\_validation.py                                           |      157 |       11 |     93% |39, 106, 114, 167-175, 282 |
| python/hsfs/core/search\_api.py                                                  |        3 |        0 |    100% |           |
| python/hsfs/core/services\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/share\_api.py                                                   |       80 |       80 |      0% |    20-292 |
| python/hsfs/core/spine\_group\_engine.py                                         |       17 |       11 |     35% |     24-49 |
| python/hsfs/core/statistics\_api.py                                              |       90 |       73 |     19% |44-53, 84-110, 141-163, 194-208, 233-242, 264-283, 296, 326-376 |
| python/hsfs/core/statistics\_comparison\_config.py                               |      116 |        9 |     92% |124-126, 151, 154, 157-162, 166 |
| python/hsfs/core/statistics\_comparison\_result.py                               |       53 |       15 |     72% |55-57, 61, 70, 73, 76-79, 85, 91, 97, 103, 109 |
| python/hsfs/core/statistics\_engine.py                                           |      127 |       11 |     91% |146, 148, 179-202, 396-397, 533, 580 |
| python/hsfs/core/storage\_connector\_api.py                                      |       56 |       39 |     30% |35-46, 60-69, 90-100, 121-126, 139, 166-176, 181-191, 198-210, 228-248, 267-287 |
| python/hsfs/core/tag\_schemas\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/tags\_api/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_api.py                                       |       59 |       40 |     32% |31-40, 53-63, 71-81, 88-94, 102-114, 132-143, 174-185, 207-219, 231-241 |
| python/hsfs/core/training\_dataset\_engine.py                                    |       60 |        1 |     98% |       178 |
| python/hsfs/core/training\_dataset\_job\_conf.py                                 |       37 |       14 |     62% |25-28, 32, 36, 40, 44, 48, 52, 56, 60, 63, 66 |
| python/hsfs/core/transformation\_execution\_dag.py                               |      179 |       25 |     86% |78, 200, 209-220, 262, 327-328, 346-360 |
| python/hsfs/core/transformation\_function\_api.py                                |       26 |       16 |     38% |42-51, 80-95, 108-118 |
| python/hsfs/core/transformation\_function\_engine.py                             |      506 |       89 |     82% |215-216, 224-228, 252, 283-287, 309, 314-327, 361-363, 384, 398, 789, 829-830, 839, 844-849, 856-862, 894, 896, 951-971, 984-1000, 1043-1053, 1108, 1203-1236, 1285-1292, 1403, 1515-1520, 1571, 1628-1633, 1666 |
| python/hsfs/core/type\_systems.py                                                |        2 |        0 |    100% |           |
| python/hsfs/core/util\_sql.py                                                    |       38 |       21 |     45% |37-74, 91-106 |
| python/hsfs/core/validation\_report\_api.py                                      |       34 |       21 |     38% |44-65, 75-87, 95-113, 123-140 |
| python/hsfs/core/validation\_report\_engine.py                                   |       41 |        5 |     88% |59, 84-86, 110 |
| python/hsfs/core/validation\_result\_api.py                                      |       12 |        4 |     67% |     50-64 |
| python/hsfs/core/validation\_result\_engine.py                                   |       33 |        3 |     91% |78, 82, 119 |
| python/hsfs/core/variable\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/vector\_db\_client.py                                           |      253 |       71 |     72% |80-83, 98, 123-127, 131, 173-196, 223, 234, 258, 294-296, 328, 354-360, 369, 379, 396, 423-424, 437-456, 476-490, 493-500, 507, 513-522, 526, 530, 534-544, 548 |
| python/hsfs/core/vector\_server.py                                               |      676 |      494 |     27% |199-259, 266-268, 275-309, 321-336, 345-359, 369, 393-458, 496-578, 624-806, 849-919, 935-964, 983-1015, 1038-1079, 1115-1168, 1189-1193, 1221-1224, 1228-1230, 1232-1236, 1239-1241, 1245, 1247, 1249, 1251, 1266, 1268, 1273, 1280, 1284-1286, 1307-1328, 1355-1381, 1402-1428, 1436-1450, 1453-1473, 1482-1506, 1531-1576, 1581-1593, 1606-1651, 1682-1699, 1733, 1765-1810, 1836-1884, 1906-1919, 1925, 1931, 1943-1947, 1951-1966, 1970-1978, 1982, 1986, 1990, 1994-2002, 2008, 2014, 2019, 2026-2040, 2046-2057, 2061-2065, 2069, 2073-2096, 2100-2120, 2125-2131, 2136-2142 |
| python/hsfs/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsfs/embedding.py                                                         |      157 |       34 |     78% |48, 61-63, 71, 79-100, 103, 106, 171, 177, 183, 217, 225-232, 235, 314-318, 348-350, 398, 406, 413 |
| python/hsfs/engine/\_\_init\_\_.py                                               |       42 |        6 |     86% |34, 41, 45, 49-50, 80 |
| python/hsfs/engine/python.py                                                     |      934 |      126 |     87% |320, 324, 347, 349-351, 355, 407, 413, 439, 445, 525-532, 629, 727, 772-783, 800, 826-830, 843-852, 892-895, 935-941, 978-987, 1033-1034, 1079, 1144-1147, 1177, 1180-1181, 1209-1210, 1236, 1255, 1271, 1346-1349, 1433, 1567-1571, 1684-1685, 1734, 1783, 1848-1860, 1868, 1871, 2033, 2084, 2092, 2114, 2129-2133, 2163, 2191, 2221-2225, 2264, 2282-2285, 2316, 2328-2332, 2566-2568, 2574, 2580, 2907, 2911-2912, 2937-2941, 2943-2951, 2967-2968, 2974-2975, 3019, 3058-3059 |
| python/hsfs/engine/spark.py                                                      |     1058 |      235 |     78% |88-93, 187-188, 205-206, 213-215, 227-233, 254, 262, 292-300, 327-337, 346, 393-401, 509, 518-522, 529-561, 599-609, 648, 667-668, 671, 724, 757-764, 857, 1061-1062, 1207, 1240-1244, 1264-1293, 1296-1313, 1316-1345, 1609, 1614, 1665-1717, 1743, 1783, 1859, 1887, 1905, 1923-1965, 2044, 2058-2077, 2081-2088, 2146, 2150, 2184, 2194-2195, 2219-2220, 2242-2243, 2246, 2328-2334, 2420-2430, 2654, 2684-2690, 2837, 2845-2859, 2863, 2979-2980, 2986-2987, 2993-2994, 3000-3001, 3014-3015, 3018, 3033 |
| python/hsfs/engine/spark\_metrics.py                                             |      115 |       26 |     77% |48-54, 78-79, 99-102, 107, 110, 128-129, 162, 185-194 |
| python/hsfs/engine/spark\_no\_metastore.py                                       |       14 |        5 |     64% | 35-44, 48 |
| python/hsfs/expectation\_suite.py                                                |      250 |       73 |     71% |53, 86, 101, 195, 241, 260, 271-276, 288, 295-310, 347, 381-385, 435-444, 474-487, 509-513, 520, 523-541, 551, 561-563, 575, 585, 595-597, 615-617, 642, 659, 663 |
| python/hsfs/feature.py                                                           |      182 |       11 |     94% |161, 201, 221, 237, 267, 300, 304, 314, 330, 368, 398 |
| python/hsfs/feature\_group.py                                                    |     1497 |      349 |     77% |171, 311, 344, 350, 594-599, 670, 882, 923, 951, 974, 1000, 1012, 1028, 1047, 1064, 1082-1094, 1109-1121, 1137, 1153, 1195-1197, 1217-1219, 1238-1240, 1277-1278, 1306-1308, 1340-1341, 1381-1396, 1430-1433, 1465-1466, 1488-1506, 1542-1545, 1588-1606, 1641-1646, 1700, 1717, 1722, 1731, 1751-1753, 1787, 1820-1822, 1873-1894, 1946-1954, 2016-2021, 2039, 2046, 2097-2102, 2157-2162, 2225-2239, 2309, 2336, 2352, 2379, 2383, 2398, 2418, 2443, 2458, 2471, 2487, 2512-2513, 2561-2562, 2600-2601, 2626-2633, 2658, 2682, 2732, 2750-2752, 2782, 2836, 2864, 2897, 2907, 2917, 2959-2960, 2967, 2979-2988, 3000, 3009, 3012-3040, 3049, 3055-3064, 3101, 3123, 3171-3172, 3200-3201, 3465, 3570, 3575-3580, 3643, 3803, 3808, 3812, 3843, 3846-3850, 3898, 3958-3967, 3994-3998, 4090, 4094, 4098, 4178, 4342, 4360, 4369, 4628-4648, 4695, 4714-4729, 4759, 4865, 4896, 4913, 5141-5143, 5167-5195, 5210, 5214, 5229, 5239, 5243-5244, 5254, 5262, 5277, 5294, 5354, 5360, 5362, 5367, 5371, 5375, 5595, 5617, 5624-5632, 5648, 5658, 5664, 5668, 5675-5678, 5695-5708, 5714-5719, 5743-5750, 5892-5893, 5983-6013, 6042-6043, 6160, 6178, 6187, 6211-6215, 6246-6250, 6306-6315, 6327, 6334, 6342-6350, 6353, 6384, 6386, 6433, 6533-6534, 6562-6563, 6602, 6612-6615, 6627-6629, 6632-6636, 6639, 6642 |
| python/hsfs/feature\_group\_commit.py                                            |       84 |       16 |     81% |61-64, 67, 70, 113, 121, 125, 129, 133, 137, 141, 145, 149, 153 |
| python/hsfs/feature\_group\_writer.py                                            |       17 |        0 |    100% |           |
| python/hsfs/feature\_logger.py                                                   |       14 |       14 |      0% |     16-38 |
| python/hsfs/feature\_logger\_async.py                                            |      128 |      128 |      0% |    16-293 |
| python/hsfs/feature\_store.py                                                    |      353 |      100 |     72% |202, 221-226, 273, 275, 301, 330-347, 371, 408-409, 435-447, 469, 496, 524, 563-565, 594-596, 622-624, 642-644, 685, 710, 730, 987, 1271-1272, 1461-1504, 1684-1688, 1694, 1841-1859, 1953-1959, 2013, 2121, 2142, 2261-2278, 2417, 2453-2455, 2459-2462, 2504-2510, 2531, 2555, 2586-2590, 2611, 2635, 2665, 2677, 2767, 2825, 2883, 2941, 2996 |
| python/hsfs/feature\_store\_activity.py                                          |       94 |       94 |      0% |    16-183 |
| python/hsfs/feature\_view.py                                                     |      892 |      263 |     71% |193, 287, 362-364, 393, 482-488, 497-498, 523, 528-531, 667, 714, 889, 896, 1061, 1068-1069, 1131, 1188, 1197-1218, 1282-1293, 1301-1313, 1461, 1513, 1540, 1564, 1581, 1601-1605, 1623-1628, 1649, 1675, 1685, 1894-1933, 2193-2238, 2486-2540, 2612-2622, 2715-2728, 2772, 3151, 3398, 3576-3589, 3658-3675, 3705-3712, 3737-3743, 3775, 3816, 3852, 3886, 3919, 3946-3948, 3972-3974, 4001-4003, 4027-4029, 4082-4090, 4100-4132, 4179-4184, 4236-4241, 4298-4312, 4378-4384, 4485-4486, 4535, 4554, 4610, 4633, 4640, 4742-4760, 4793, 4834-4837, 4883-4885, 4894-4901, 5057-5078, 5130, 5180, 5205, 5220, 5244, 5264-5265, 5288-5297, 5470-5473, 5478-5491, 5497-5498, 5544, 5582, 5592, 5598, 5608, 5618, 5623, 5676, 5686, 5748, 5759, 5794-5809, 5840-5842, 5853, 5867, 5877, 5883-5885, 6001 |
| python/hsfs/ge\_expectation.py                                                   |      101 |       13 |     87% |40, 68, 120, 123, 126, 144, 158-161, 171, 186, 201 |
| python/hsfs/ge\_validation\_result.py                                            |      146 |       18 |     88% |52, 111, 157, 167, 182, 193, 199, 214, 233, 273, 286, 292, 295-301 |
| python/hsfs/hopsworks\_udf.py                                                    |      454 |       12 |     97% |143, 407-409, 637, 748, 754, 954, 969, 971, 1201, 1492 |
| python/hsfs/online\_config.py                                                    |       91 |        1 |     99% |       137 |
| python/hsfs/serving\_key.py                                                      |       60 |       10 |     83% |57, 94-98, 108, 113, 123, 128 |
| python/hsfs/split\_statistics.py                                                 |       28 |        2 |     93% |    57, 65 |
| python/hsfs/statistics.py                                                        |      115 |       27 |     77% |85, 87, 89, 102, 136-149, 152, 155, 158, 170, 174-182, 196, 202, 208, 214, 226 |
| python/hsfs/statistics\_config.py                                                |       81 |        6 |     93% |54, 65, 67, 118, 141, 144 |
| python/hsfs/storage\_connector.py                                                |     1971 |      410 |     79% |150, 174-181, 283, 322, 341-350, 354, 368, 385-388, 400-411, 428-431, 443-455, 474-480, 507, 510, 512, 514, 529, 531-537, 554-561, 574, 615, 713-719, 754-756, 761-774, 809, 815-816, 853, 1011-1036, 1218, 1251-1263, 1277, 1341, 1422, 1462, 1618, 1630, 1651, 1694-1708, 1717, 1719-1720, 1739-1753, 1794, 1815, 1964, 2023, 2042, 2157, 2229, 2276-2285, 2292, 2384-2393, 2649-2658, 2665, 2667, 2698-2699, 2772, 2816-2825, 2916, 2933, 3016, 3151-3152, 3237-3248, 3344, 3349, 3354, 3377-3378, 3392, 3408, 3538-3549, 3589-3598, 3607, 3612, 3617, 3622, 3627, 3632, 3637, 3640-3658, 3661, 3665-3684, 3695-3719, 3747, 3806, 3810, 3814, 3818, 3822, 3826, 3830, 3834, 3838, 3842, 3846, 3850, 3855, 3860, 3887, 4138, 4163-4167, 4174-4182, 4190, 4205-4299, 4311-4316, 4350-4355, 4361, 4367, 4373, 4379, 4385, 4391, 4406-4408, 4427-4429, 4433-4451, 4475-4476, 4480, 4484, 4487, 4497-4498, 4503-4513, 4517, 4521, 4524, 4562-4570, 4573-4577, 4585, 4589, 4593, 4597, 4601, 4605, 4609, 4613, 4617, 4621, 4624-4642, 4680-4684, 4693, 4695-4702, 4706, 4710, 4714-4716, 4719-4730, 4733, 4912, 4928-4929, 4934-4935, 4988, 5021-5022, 5058, 5071 |
| python/hsfs/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsfs/training\_dataset.py                                                 |      476 |      117 |     75% |247, 252-270, 279, 288, 297, 345, 370, 375, 385, 412, 430, 440, 442, 444, 448, 488, 500, 509, 518, 527, 536, 545, 554, 563, 568, 572, 716-734, 776-787, 806-811, 816-837, 850, 866, 878, 893, 905, 920-921, 929-930, 949-954, 960-970, 972, 981-989, 992-1001, 1011, 1016, 1044, 1055, 1061, 1065, 1081, 1087, 1100, 1117, 1135-1137, 1155-1157, 1170, 1182, 1188-1196 |
| python/hsfs/training\_dataset\_feature.py                                        |       80 |        8 |     90% |59, 88, 134, 150, 160, 163-166 |
| python/hsfs/training\_dataset\_split.py                                          |       54 |        7 |     87% |52, 60, 68, 76, 84, 87, 90 |
| python/hsfs/transformation\_function.py                                          |      155 |       15 |     90% |138, 169, 231-233, 241, 548, 575, 585, 616, 641-645 |
| python/hsfs/transformation\_statistics.py                                        |      139 |       16 |     88% |96, 102, 110, 116, 122, 128, 148, 172, 190, 204, 214, 220, 226, 238, 290, 297 |
| python/hsfs/usage.py                                                             |        2 |        0 |    100% |           |
| python/hsfs/user/\_\_init\_\_.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/util.py                                                              |       77 |        9 |     88% |91, 116, 242-256 |
| python/hsfs/validation\_report.py                                                |      138 |       16 |     88% |72, 104, 148, 164, 174, 227, 244, 263, 281-287, 293, 296 |
| python/hsfs/version.py                                                           |        2 |        0 |    100% |           |
| python/hsml/\_\_init\_\_.py                                                      |       14 |        1 |     93% |        37 |
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
| python/hsml/core/hdfs\_api.py                                                    |       21 |       12 |     43% |28, 52-76, 88 |
| python/hsml/core/huggingface\_api.py                                             |       28 |       18 |     36% |37-38, 75-86, 103-104, 119-121 |
| python/hsml/core/model\_api.py                                                   |       87 |       57 |     34% |40-50, 70-80, 108-124, 146-174, 182-191, 204-217, 228-239, 322-346, 365-389 |
| python/hsml/core/model\_registry\_api.py                                         |       24 |       16 |     33% | 27, 38-62 |
| python/hsml/core/model\_serving\_api.py                                          |       53 |       20 |     62% |38-46, 51-63, 72-73, 120-127 |
| python/hsml/core/serving\_api.py                                                 |      163 |      114 |     30% |52-67, 79-89, 103-118, 134-145, 158-167, 230-233, 244-258, 267-275, 283-290, 303-311, 324-334, 352-358, 366-389, 401-421, 424-428, 436-439, 450-458, 469-474, 505-524 |
| python/hsml/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsml/deployable\_component.py                                             |       56 |        5 |     91% |73, 95, 105, 115, 125 |
| python/hsml/deployable\_component\_logs.py                                       |       57 |        6 |     89% |75, 110, 114, 119, 123, 126 |
| python/hsml/deployment.py                                                        |      355 |       58 |     84% |351, 432-440, 533, 586, 681, 711, 732, 738, 748, 754, 764, 770, 778, 782, 788, 792, 798, 802, 812, 816, 822, 832, 838, 842, 848, 852, 858, 862, 872, 876, 882, 886, 892, 896, 902, 906, 912, 916, 932, 936, 942, 948, 954, 958, 964, 968, 984, 988, 994, 998, 1004, 1008, 1011-1016 |
| python/hsml/deployment\_tracing\_config.py                                       |       93 |       11 |     88% |66, 71, 124-126, 129, 153, 163, 177, 191, 196 |
| python/hsml/engine/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/engine/local\_engine.py                                              |       46 |       26 |     43% |35-36, 39, 48-66, 84-103, 106-108, 111-113, 116, 119-122 |
| python/hsml/engine/model\_engine.py                                              |      408 |      232 |     43% |82-102, 108-135, 145, 157-158, 168, 174-185, 203-232, 238-245, 258-278, 319-403, 406, 416-539, 592-594, 612, 617, 663-664, 669, 681, 684-685, 717-724, 735-738, 740, 746, 753-754, 770-771, 781-782, 814-816, 840-865, 868-885, 888-902, 905, 914, 924, 933, 942, 950, 967, 984 |
| python/hsml/engine/serving\_engine.py                                            |      440 |      320 |     27% |82, 91, 100, 108, 113-150, 153-205, 208-246, 249-288, 291-311, 314-345, 348-351, 358-363, 366-367, 378-407, 413-420, 423-478, 481-508, 511-556, 611-625, 628-635, 638-663, 758-760, 797, 852-884, 894-902, 911-973, 985-1006, 1021-1025, 1030-1062 |
| python/hsml/inference\_batcher.py                                                |       77 |       10 |     87% |52, 83-85, 88, 108, 118, 128, 138, 141 |
| python/hsml/inference\_endpoint.py                                               |       84 |        7 |     92% |54, 67, 109-111, 133, 155 |
| python/hsml/inference\_logger.py                                                 |       66 |        8 |     88% |51, 93-95, 98, 114, 124, 127 |
| python/hsml/kafka\_topic/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/llm/\_\_init\_\_.py                                                  |        0 |        0 |    100% |           |
| python/hsml/llm/model.py                                                         |       15 |        8 |     47% |     73-80 |
| python/hsml/llm/predictor.py                                                     |       14 |        0 |    100% |           |
| python/hsml/llm/signature.py                                                     |       13 |        4 |     69% |     79-94 |
| python/hsml/model.py                                                             |      352 |       61 |     83% |142-147, 153-168, 300, 341, 435, 474, 507-508, 582, 611, 626, 655-658, 720-728, 739, 751-757, 760, 791, 801, 811, 821, 831, 841, 851, 855, 865, 875, 879, 885, 889, 901, 911, 923, 933, 943, 952, 961, 970, 976, 980, 985, 988 |
| python/hsml/model\_registry.py                                                   |      192 |       25 |     87% |79-80, 100-105, 133, 160-169, 175, 181, 187, 316, 321, 333, 360, 412, 427, 439, 445, 451, 457, 463, 466-471 |
| python/hsml/model\_schema.py                                                     |       20 |        5 |     75% |50, 57, 60-66 |
| python/hsml/model\_serving.py                                                    |      258 |       32 |     88% |40-41, 103, 131, 175-179, 182-190, 199, 367, 530, 578, 720, 726, 732, 735, 849, 872, 879, 889, 894-895, 922, 924, 926, 928, 930 |
| python/hsml/predictor.py                                                         |      452 |       45 |     90% |51, 187, 275, 283-285, 383, 487, 497, 507, 517-518, 528, 532, 539, 552-553, 569, 579, 593, 603, 613, 633, 643, 653, 671-674, 684, 704, 710, 714, 724, 734, 744, 846-858 |
| python/hsml/predictor\_state.py                                                  |       87 |       14 |     84% |53, 83-103, 160 |
| python/hsml/predictor\_state\_condition.py                                       |       50 |        7 |     86% |42, 62-64, 67, 70, 97 |
| python/hsml/python/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/python/endpoint.py                                                   |       10 |        0 |    100% |           |
| python/hsml/python/model.py                                                      |       15 |        1 |     93% |        76 |
| python/hsml/python/predictor.py                                                  |        9 |        5 |     44% |     25-33 |
| python/hsml/python/signature.py                                                  |       13 |        0 |    100% |           |
| python/hsml/resources.py                                                         |      153 |       13 |     92% |51, 71, 84, 94, 104, 107, 149, 168, 203, 207, 237, 247, 250 |
| python/hsml/scaling\_config.py                                                   |      209 |       28 |     87% |104, 108-109, 211-213, 217, 244, 254-263, 275, 285, 295, 305, 315, 325, 335, 338, 365, 379, 406, 416, 421 |
| python/hsml/schema.py                                                            |       30 |        3 |     90% |72, 79, 82 |
| python/hsml/sklearn/\_\_init\_\_.py                                              |        0 |        0 |    100% |           |
| python/hsml/sklearn/model.py                                                     |       15 |        8 |     47% |     73-80 |
| python/hsml/sklearn/predictor.py                                                 |        7 |        3 |     57% |     25-28 |
| python/hsml/sklearn/signature.py                                                 |       13 |        4 |     69% |     79-94 |
| python/hsml/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsml/tensorflow/\_\_init\_\_.py                                           |        0 |        0 |    100% |           |
| python/hsml/tensorflow/model.py                                                  |       15 |        8 |     47% |     73-80 |
| python/hsml/tensorflow/predictor.py                                              |        9 |        5 |     44% |     25-33 |
| python/hsml/tensorflow/signature.py                                              |       13 |        4 |     69% |     79-94 |
| python/hsml/torch/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/torch/model.py                                                       |       15 |        8 |     47% |     73-80 |
| python/hsml/torch/predictor.py                                                   |        9 |        5 |     44% |     25-33 |
| python/hsml/torch/signature.py                                                   |       13 |        4 |     69% |     79-94 |
| python/hsml/transformer.py                                                       |       69 |        7 |     90% |33, 74, 124-127, 146 |
| python/hsml/util/\_\_init\_\_.py                                                 |       18 |       18 |      0% |      5-22 |
| python/hsml/utils/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/utils/local\_paths.py                                                |       50 |        1 |     98% |       133 |
| python/hsml/utils/schema/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hsml/utils/schema/column.py                                               |        7 |        0 |    100% |           |
| python/hsml/utils/schema/columnar\_schema.py                                     |       61 |        0 |    100% |           |
| python/hsml/utils/schema/tensor.py                                               |        8 |        0 |    100% |           |
| python/hsml/utils/schema/tensor\_schema.py                                       |       34 |        0 |    100% |           |
| python/hsml/version.py                                                           |        2 |        2 |      0% |     17-22 |
| **TOTAL**                                                                        | **40929** | **12515** | **69%** |           |


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