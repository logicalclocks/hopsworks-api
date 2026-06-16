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
| python/hopsworks/cli/commands/transformation.py                                  |       84 |       13 |     85% |36-37, 92, 97-98, 109-110, 167, 174-175, 178, 185, 188 |
| python/hopsworks/cli/commands/trino.py                                           |      134 |       80 |     40% |45, 64-65, 70-71, 76-85, 90-95, 108-110, 114-116, 127-154, 220-231, 251, 264, 279-285, 300-305, 319-330 |
| python/hopsworks/cli/commands/update.py                                          |       49 |       15 |     69% |54, 69-70, 73, 76, 87-96, 107-108 |
| python/hopsworks/cli/config.py                                                   |      155 |       22 |     86% |75, 89, 105, 109-110, 119-121, 140, 144, 149-151, 168-169, 245, 247, 331-337 |
| python/hopsworks/cli/joinspec.py                                                 |       18 |        0 |    100% |           |
| python/hopsworks/cli/lineage.py                                                  |       33 |        4 |     88% | 55, 62-64 |
| python/hopsworks/cli/main.py                                                     |       61 |        5 |     92% |57-58, 66, 166, 235 |
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
| python/hopsworks/core/\_\_init\_\_.py                                            |        5 |        5 |      0% |       5-9 |
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
| python/hopsworks/mcp/run\_server.py                                              |       56 |       27 |     52% |35-39, 47-49, 128-167 |
| python/hopsworks/mcp/server.py                                                   |       21 |        1 |     95% |        54 |
| python/hopsworks/mcp/tools/\_\_init\_\_.py                                       |        7 |        0 |    100% |           |
| python/hopsworks/mcp/tools/auth.py                                               |       15 |        4 |     73% |     78-95 |
| python/hopsworks/mcp/tools/brewer.py                                             |       79 |       58 |     27% |64-101, 108-130, 134-171 |
| python/hopsworks/mcp/tools/dataset.py                                            |       72 |       49 |     32% |75-89, 122-137, 170-184, 222-239, 267-273, 288-297 |
| python/hopsworks/mcp/tools/feature\_group.py                                     |       67 |       45 |     33% |60-67, 72-81, 87-95, 108-111, 120-125, 150-179, 190-195 |
| python/hopsworks/mcp/tools/jobs.py                                               |       25 |       11 |     56% |59-66, 81-89 |
| python/hopsworks/mcp/tools/project.py                                            |       58 |       36 |     38% |63-64, 76-90, 114-134, 151-157, 179-183, 203-208 |
| python/hopsworks/mcp/tools/terminal.py                                           |       46 |       25 |     46% |35-37, 67-90, 102-106, 120-126, 139-140 |
| python/hopsworks/mcp/utils/\_\_init\_\_.py                                       |        0 |        0 |    100% |           |
| python/hopsworks/mcp/utils/auth.py                                               |       11 |        6 |     45% |     57-76 |
| python/hopsworks/mcp/utils/tags.py                                               |       16 |        0 |    100% |           |
| python/hopsworks/project/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hopsworks/secret/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/spark.py                                                        |       17 |       17 |      0% |     18-95 |
| python/hopsworks/tag/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/triggered\_alert/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/user/\_\_init\_\_.py                                            |        2 |        2 |      0% |       5-6 |
| python/hopsworks/util/\_\_init\_\_.py                                            |       30 |       30 |      0% |      5-34 |
| python/hopsworks/version.py                                                      |        2 |        2 |      0% |     17-22 |
| python/hopsworks\_common/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hopsworks\_common/alert.py                                                |      150 |       58 |     61% |37-42, 46-51, 57, 63, 69, 75, 81, 87, 90, 98, 107, 110, 130-141, 147, 153, 159, 163, 175, 194-204, 210, 216, 220, 231, 251-262, 268, 274, 280, 284, 296, 317-329, 335, 341, 347, 353, 357, 370 |
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
| python/hopsworks\_common/constants.py                                            |      182 |        2 |     99% |    25, 28 |
| python/hopsworks\_common/core/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks\_common/core/alerts\_api.py                                     |      262 |      170 |     35% |130-133, 165-168, 197-200, 233-243, 277-288, 327-339, 375-388, 429-443, 485-517, 561-594, 640-665, 706-730, 758-762, 796-801, 844-909, 933-937, 980-1012, 1045-1049, 1061-1064, 1072-1117, 1129-1133, 1136-1149 |
| python/hopsworks\_common/core/app\_api.py                                        |      148 |       38 |     74% |52-58, 75-89, 172, 175, 185, 192, 194, 254-269, 273-284, 338-345, 351 |
| python/hopsworks\_common/core/constants.py                                       |       32 |        0 |    100% |           |
| python/hopsworks\_common/core/dataset.py                                         |       31 |       14 |     55% |33-37, 41-44, 48, 52, 56, 60, 64 |
| python/hopsworks\_common/core/dataset\_api.py                                    |      365 |      247 |     32% |106-168, 217-288, 300-366, 388, 405-408, 411, 422-426, 456, 471-472, 486, 499-501, 515, 559-583, 614-632, 658-667, 695-713, 739-757, 775-798, 830-849, 870-887, 903-918, 934-938, 967-1019, 1038, 1062, 1086-1098, 1112-1122, 1139-1155 |
| python/hopsworks\_common/core/env\_var\_api.py                                   |       59 |        1 |     98% |       254 |
| python/hopsworks\_common/core/environment\_api.py                                |       42 |       22 |     48% |63-86, 109-114, 147-152, 164-174 |
| python/hopsworks\_common/core/execution\_api.py                                  |       53 |       18 |     66% |65, 89-100, 105-111, 119-128, 131-141, 153-155 |
| python/hopsworks\_common/core/git\_api.py                                        |      177 |      130 |     27% |87-122, 135-142, 157, 176, 208-211, 228-254, 257-265, 268-291, 294-316, 321-344, 347-380, 383-410, 413-440, 443-470, 473-499, 502-529, 532-556, 559-572, 578-584 |
| python/hopsworks\_common/core/git\_op\_execution\_api.py                         |        9 |        4 |     56% |     24-36 |
| python/hopsworks\_common/core/git\_provider\_api.py                              |       45 |       31 |     31% |31-34, 39-44, 49-67, 70-81, 88-96 |
| python/hopsworks\_common/core/git\_remote\_api.py                                |       35 |       25 |     29% |25, 28-43, 46-61, 64-88, 91-113 |
| python/hopsworks\_common/core/hosts\_api.py                                      |       10 |        3 |     70% |     28-32 |
| python/hopsworks\_common/core/ingestion\_job.py                                  |       19 |        0 |    100% |           |
| python/hopsworks\_common/core/ingestion\_job\_conf.py                            |       39 |       14 |     64% |33-36, 40, 44, 48, 52, 56, 60, 64, 68, 71, 74 |
| python/hopsworks\_common/core/inode.py                                           |       41 |        6 |     85% |51, 55, 59, 67, 71, 75 |
| python/hopsworks\_common/core/job\_api.py                                        |      106 |       54 |     49% |78-91, 108-116, 131-138, 156-157, 176-186, 194-201, 213-220, 228-233, 240-243, 259-263, 292-298, 310-315, 324-327 |
| python/hopsworks\_common/core/job\_configuration.py                              |       21 |        1 |     95% |        75 |
| python/hopsworks\_common/core/kafka\_api.py                                      |       99 |       56 |     43% |70-82, 128-149, 165-170, 183-186, 196-204, 213-223, 236-243, 259-275, 292-296, 305-316, 321-330, 340, 369-389 |
| python/hopsworks\_common/core/library\_api.py                                    |       15 |        4 |     73% |     41-54 |
| python/hopsworks\_common/core/opensearch.py                                      |      240 |       75 |     69% |44, 52-98, 126, 161, 194, 205-208, 212-218, 223-230, 284-286, 295, 372-407, 450-453, 458-470, 511 |
| python/hopsworks\_common/core/opensearch\_api.py                                 |       54 |       15 |     72% |62-73, 86-87, 113-114, 135-140 |
| python/hopsworks\_common/core/project\_api.py                                    |       58 |       32 |     45% |39-43, 54-64, 75-79, 90-94, 108-115, 167-173, 176-179 |
| python/hopsworks\_common/core/rest.py                                            |       18 |        1 |     94% |        63 |
| python/hopsworks\_common/core/rest\_endpoint.py                                  |      236 |       59 |     75% |48-51, 60, 63, 68, 102, 106-109, 115, 124, 130, 141, 144-146, 152, 157, 167, 182-183, 186, 201-203, 206, 271-277, 280, 305-310, 313, 325, 333, 366, 382, 385, 410-413, 418, 429-433, 440, 450 |
| python/hopsworks\_common/core/search\_api.py                                     |      133 |       82 |     38% |45-47, 53, 59, 65, 69, 72, 84-103, 110, 115, 119, 122, 134-138, 237, 299-308, 362-371, 425-434, 485-494, 510-525, 543-562, 576-624 |
| python/hopsworks\_common/core/secret\_api.py                                     |       61 |       16 |     74% |57-62, 92-93, 119-125, 161-163, 231-237 |
| python/hopsworks\_common/core/services\_api.py                                   |       10 |        3 |     70% |     30-35 |
| python/hopsworks\_common/core/sink\_job\_configuration.py                        |      314 |       52 |     83% |67, 95-96, 99, 106-107, 115, 119, 124, 128, 152, 186-193, 195, 203, 249, 285, 294, 346-358, 367-375, 380, 449, 460, 469, 478, 487, 496, 505, 529, 538, 547, 557, 569, 581 |
| python/hopsworks\_common/core/superset\_api.py                                   |      156 |       45 |     71% |110-111, 210, 240-252, 265, 275, 290, 303, 337-350, 363, 373, 388, 401, 433-447, 460, 470, 485, 500 |
| python/hopsworks\_common/core/tag\_schemas\_api.py                               |       50 |       50 |      0% |    23-179 |
| python/hopsworks\_common/core/tags\_api.py                                       |       39 |       20 |     49% |62-68, 86-91, 113-119, 128-149 |
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
| python/hopsworks\_common/execution.py                                            |      146 |       24 |     84% |73, 79-81, 93, 123, 141, 147, 153, 159, 165, 171, 185, 190, 210, 239, 252, 265, 284, 287, 290, 295-303 |
| python/hopsworks\_common/git\_commit.py                                          |       53 |       26 |     51% |40-49, 53-60, 66, 72, 78, 84, 90, 93, 96, 99 |
| python/hopsworks\_common/git\_file\_status.py                                    |       37 |       15 |     59% |34-36, 40-45, 51, 69, 75, 78, 81, 84 |
| python/hopsworks\_common/git\_op\_execution.py                                   |       54 |       25 |     54% |44-52, 56-57, 62, 67, 72, 77, 82, 87, 92, 97, 102, 107-114 |
| python/hopsworks\_common/git\_provider.py                                        |       40 |       15 |     62% |42-45, 50-53, 59, 65, 71, 81, 84, 87, 90 |
| python/hopsworks\_common/git\_remote.py                                          |       37 |       15 |     59% |40-43, 47-52, 58, 64, 74, 77, 80, 83 |
| python/hopsworks\_common/git\_repo.py                                            |      129 |       50 |     61% |52-68, 72-77, 83, 89, 95, 101, 107, 113, 119, 125, 137, 150, 164-167, 179, 192, 205, 220, 234, 248, 262, 277, 292, 321, 336, 348, 351, 354, 357 |
| python/hopsworks\_common/job.py                                                  |      198 |       59 |     70% |81-88, 113, 119, 123, 135, 141, 159, 250, 305-309, 329-333, 345, 362, 375, 381-398, 409, 482-498, 504-505, 511-521, 527-537, 550, 566, 599, 602-605, 608, 611, 614, 619-621 |
| python/hopsworks\_common/job\_schedule.py                                        |       86 |       10 |     88% |43, 112, 130, 148, 154, 160, 170, 176, 182, 188 |
| python/hopsworks\_common/kafka\_schema.py                                        |       51 |       22 |     57% |36-41, 45-50, 53-55, 61, 67, 73, 79, 92, 95, 98, 101 |
| python/hopsworks\_common/kafka\_topic.py                                         |      106 |       18 |     83% |70, 97-99, 118-120, 130, 136, 146, 152, 162, 168, 183, 186, 189, 198, 201 |
| python/hopsworks\_common/library.py                                              |       19 |       11 |     42% |39-50, 54-55 |
| python/hopsworks\_common/project.py                                              |      145 |       43 |     70% |98-101, 107, 113, 119, 125, 131, 137, 232-235, 244-247, 256, 274, 279, 288, 297, 306, 315, 324, 333-335, 344-346, 358, 370, 400, 436, 441, 444, 447-449, 454-455 |
| python/hopsworks\_common/search\_results.py                                      |      224 |       40 |     82% |58, 61, 110, 116, 122, 146, 156-172, 199, 217, 235, 243, 253-262, 398, 404, 410, 416, 448, 464 |
| python/hopsworks\_common/secret.py                                               |       61 |       11 |     82% |61, 80, 86, 92, 98, 110, 113, 116, 119-121 |
| python/hopsworks\_common/spark\_connect\_utils.py                                |       38 |        6 |     84% |66-67, 103-104, 110-111 |
| python/hopsworks\_common/tag.py                                                  |       80 |       16 |     80% |60-63, 69, 88, 91-94, 109, 120, 141, 153, 156, 159 |
| python/hopsworks\_common/triggered\_alert.py                                     |       87 |       39 |     55% |26-28, 32-35, 38, 41, 48, 51, 71-79, 83-88, 94, 100, 106, 112, 118, 124, 130, 136, 142, 145, 148, 161, 164 |
| python/hopsworks\_common/usage.py                                                |      179 |      105 |     41% |48-52, 55-57, 60-62, 65-67, 70-72, 75-85, 88-90, 93, 96, 99-101, 104, 124-125, 128-129, 132-139, 142, 168, 173, 177, 182-184, 188-189, 193-197, 201-203, 207-210, 214-222, 230-255, 259-286, 290-294 |
| python/hopsworks\_common/user.py                                                 |       42 |        5 |     88% |54-55, 60, 65, 68 |
| python/hopsworks\_common/util.py                                                 |      530 |      136 |     74% |67-68, 80-102, 105-108, 168-171, 280, 322, 327, 438, 458, 501, 505-508, 514-530, 536-542, 547-549, 569, 597, 623, 735, 741-743, 751-756, 801, 810-823, 860-865, 870, 874, 879, 883, 888, 916-924, 928-956, 960-962, 966-983, 995-1002, 1007, 1012, 1017 |
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
| python/hsfs/constructor/prepared\_statement\_parameter.py                        |       35 |        8 |     77% |46-48, 51, 54, 57, 69, 73 |
| python/hsfs/constructor/query.py                                                 |      351 |       70 |     80% |112-114, 331, 336, 343, 360-362, 390-395, 425, 624-630, 693-698, 722, 738, 756, 788-806, 824-828, 851-853, 859, 862, 864, 867, 870-876, 913, 917, 955, 983-986, 1090-1097, 1132-1133, 1135, 1144-1145, 1156 |
| python/hsfs/constructor/serving\_prepared\_statement.py                          |       65 |       19 |     71% |61-63, 66, 69, 76-82, 86, 90, 96, 100, 104, 108, 112, 136, 140 |
| python/hsfs/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsfs/core/arrow\_flight\_client.py                                        |      312 |      152 |     51% |30, 71-75, 79, 83, 89, 95, 103, 109, 116, 161, 171, 212-213, 228-236, 243-262, 270-276, 281-295, 302, 307, 342-346, 354-361, 371-374, 380-387, 390-393, 396-399, 402-404, 407-409, 417-426, 432-454, 467-469, 482-498, 503-505, 528-529, 554-580, 588, 592, 597, 601, 615, 620 |
| python/hsfs/core/chart.py                                                        |      101 |       39 |     61% |47-55, 59-66, 69, 82, 87, 91, 96, 100, 105, 109, 114, 118, 123, 127, 132, 136, 141, 145, 150, 154, 159, 163, 172-174, 185-187 |
| python/hsfs/core/chart\_api.py                                                   |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/constants.py                                                    |        2 |        0 |    100% |           |
| python/hsfs/core/dashboard.py                                                    |       53 |       21 |     60% |41-43, 47-54, 57, 64, 69, 73, 78, 82, 87, 91, 100-102, 113-115 |
| python/hsfs/core/dashboard\_api.py                                               |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/data\_source.py                                                 |      169 |       22 |     87% |122, 129, 132, 166, 176, 216, 226, 255, 279, 304, 323, 359, 375, 387, 403, 415, 420, 424, 429, 433, 481, 483 |
| python/hsfs/core/data\_source\_api.py                                            |       78 |       38 |     51% |43-55, 60-71, 78-104, 112-116, 124-138, 154-170, 175-189, 194-208 |
| python/hsfs/core/data\_source\_data.py                                           |       48 |        9 |     81% |56-61, 67, 85, 91, 97, 103 |
| python/hsfs/core/dataset\_api/\_\_init\_\_.py                                    |        2 |        0 |    100% |           |
| python/hsfs/core/delta\_engine.py                                                |      423 |      119 |     72% |57-61, 80-99, 135-136, 194-198, 227-241, 247, 262, 272, 278-312, 320, 342-352, 361-364, 395-397, 431, 433-442, 444-453, 455-462, 538-539, 554-558, 561-566, 578, 673, 680, 733-748, 765-798, 891 |
| python/hsfs/core/deltastreamer\_jobconf.py                                       |       16 |        6 |     62% | 30, 37-44 |
| python/hsfs/core/execution/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/expectation\_api.py                                             |       32 |       19 |     41% |46-61, 74-90, 100-114, 125-139, 149-162 |
| python/hsfs/core/expectation\_engine.py                                          |       25 |        2 |     92% |    61, 67 |
| python/hsfs/core/expectation\_suite\_api.py                                      |       53 |       39 |     26% |44-64, 77-100, 115-140, 150-168, 176-187 |
| python/hsfs/core/expectation\_suite\_engine.py                                   |       37 |        2 |     95% |    68, 82 |
| python/hsfs/core/explicit\_provenance.py                                         |      229 |      152 |     34% |35, 38, 41, 44, 57-63, 69, 75, 81, 84, 87, 98, 101, 109-123, 137-140, 149, 159, 175, 184, 188, 207, 210, 217-234, 238-244, 248-261, 265-282, 286-303, 307-352, 372-434, 439-486 |
| python/hsfs/core/external\_feature\_group\_engine.py                             |       57 |       18 |     68% |36, 79, 93-130, 203-205 |
| python/hsfs/core/feature\_descriptive\_statistics.py                             |      180 |       21 |     88% |104, 107-108, 124-125, 173, 179, 212, 215, 218, 248, 254, 260, 266, 284, 296, 302, 312, 326, 336, 342 |
| python/hsfs/core/feature\_group\_api.py                                          |      146 |       92 |     37% |51-63, 77-89, 159-165, 173-175, 192-214, 230-250, 263-273, 292-306, 335-346, 370-381, 406-421, 439-451, 469-481, 504-521, 542-559, 583-600, 624-641 |
| python/hsfs/core/feature\_group\_base\_engine.py                                 |       77 |       19 |     75% |78, 92, 109-112, 129-132, 149-152, 169-172, 216 |
| python/hsfs/core/feature\_group\_engine.py                                       |      248 |       34 |     86% |133, 215-229, 264-265, 376, 388-395, 514-516, 529-531, 544-545, 585-586, 658, 668, 707, 724, 757-764, 813, 842, 849 |
| python/hsfs/core/feature\_logging.py                                             |       75 |       34 |     55% |29, 58-61, 65-81, 89-91, 96, 101, 106, 123-134, 139, 142, 150, 153 |
| python/hsfs/core/feature\_logging\_client.py                                     |       52 |       52 |      0% |    16-109 |
| python/hsfs/core/feature\_monitoring\_config.py                                  |      278 |       92 |     67% |55, 59, 63-65, 72-74, 77, 80, 160-215, 218, 221, 224, 318-329, 361-365, 398-402, 444-451, 470-485, 505, 531-536, 559-565, 586-591, 610, 631, 634-646, 681-686, 703, 709, 715, 721, 752, 754, 756, 770, 772, 794, 808, 822, 826, 840, 850, 858, 878, 883, 893, 915, 926 |
| python/hsfs/core/feature\_monitoring\_config\_api.py                             |       72 |       47 |     35% |63-70, 86-95, 108-114, 128-134, 150-156, 176-181, 191-197, 213-219, 233-240, 262-290 |
| python/hsfs/core/feature\_monitoring\_config\_engine.py                          |      103 |       53 |     49% |142, 145, 148, 151, 160, 178, 180, 184, 190, 192, 199-202, 219-224, 241-246, 255, 284-308, 322-324, 338, 354-383, 422, 470-474 |
| python/hsfs/core/feature\_monitoring\_result.py                                  |      117 |       28 |     76% |101-125, 128, 131, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206, 212, 218, 224 |
| python/hsfs/core/feature\_monitoring\_result\_api.py                             |       44 |       25 |     43% |62-69, 82-88, 108-116, 132-138, 154-173 |
| python/hsfs/core/feature\_monitoring\_result\_engine.py                          |      120 |       19 |     84% |215-235, 371, 428, 435, 464, 496 |
| python/hsfs/core/feature\_store\_activity\_api.py                                |       12 |       12 |      0% |     16-73 |
| python/hsfs/core/feature\_store\_api.py                                          |       12 |        3 |     75% |     32-34 |
| python/hsfs/core/feature\_view\_api.py                                           |      169 |       97 |     43% |71-72, 82-83, 105-122, 139-155, 160-166, 173-175, 180-182, 228, 258-271, 281-283, 292-295, 302-303, 314-318, 325-326, 331-334, 337-338, 343-347, 352-361, 385-399, 426-440, 453-462, 469-477, 484-492, 499-514, 524-531, 541-553 |
| python/hsfs/core/feature\_view\_engine.py                                        |      528 |      170 |     68% |104-106, 154-162, 171-179, 205-206, 274, 380, 388-394, 396-401, 403-410, 433-440, 443, 471, 635-653, 657, 661, 679-680, 744-748, 805, 821, 830, 932, 944-950, 953-956, 1010-1024, 1050, 1052, 1082-1083, 1095, 1108-1113, 1138, 1162-1167, 1186-1193, 1201-1202, 1273-1276, 1310-1313, 1382, 1437-1453, 1456, 1459-1462, 1561-1656, 1916, 1920, 1938-1983, 1989-1993, 1996-2020, 2023-2029, 2038-2041, 2044, 2047, 2052-2066, 2069-2070 |
| python/hsfs/core/great\_expectation\_engine.py                                   |       43 |        4 |     91% |75, 95-100 |
| python/hsfs/core/hosts\_api/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/hudi\_engine.py                                                 |      123 |       11 |     91% |152, 218-223, 241-245, 254-258 |
| python/hsfs/core/iceberg\_engine.py                                              |      506 |      115 |     77% |91, 109, 117, 132-133, 142, 155, 165, 169, 172, 270, 280-288, 303-311, 419, 421, 430-432, 437, 463, 483-512, 536-562, 647-652, 680-683, 688-701, 711-737, 837-856, 875-884, 907, 953-955, 992-993, 1002, 1118-1121, 1123-1124 |
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
| python/hsfs/core/monitoring\_window\_config.py                                   |      143 |       40 |     72% |50, 54-56, 63-65, 68, 71, 168-169, 172-189, 192, 195, 198, 204, 215, 221-232, 249, 254-258, 275, 292, 314, 319 |
| python/hsfs/core/monitoring\_window\_config\_engine.py                           |       98 |       29 |     70% |41, 55-66, 76, 85, 125, 163, 192, 240-311, 352-363 |
| python/hsfs/core/online\_ingestion.py                                            |       84 |       35 |     58% |97, 102, 105-110, 115-116, 124, 132, 144, 153, 161, 167, 181-219, 229-256 |
| python/hsfs/core/online\_ingestion\_api.py                                       |       14 |        7 |     50% |52-64, 93-104 |
| python/hsfs/core/online\_ingestion\_result.py                                    |       40 |       18 |     55% |48-50, 64-75, 83, 95, 101, 107, 113 |
| python/hsfs/core/online\_store\_rest\_client\_api.py                             |       58 |       31 |     47% |38-53, 97-101, 142-146, 161-168, 187-192 |
| python/hsfs/core/online\_store\_rest\_client\_engine.py                          |      162 |       46 |     72% |60, 85, 90, 112, 140, 161, 175-186, 230-256, 298-301, 313, 317, 328, 374, 385, 392, 396, 404-424, 429, 433, 447, 464, 475, 479, 483, 487, 491, 509 |
| python/hsfs/core/online\_store\_sql\_engine.py                                   |      379 |      286 |     25% |79-103, 108-109, 128-179, 192-214, 224-254, 268-289, 292-319, 342-348, 374-380, 394, 409, 417-477, 485-595, 598-609, 612, 615-626, 641-643, 653-660, 666-682, 690-712, 715, 727-732, 742-757, 767-805, 809, 814, 818, 829, 836, 843, 850, 855, 859-861, 868, 873, 882, 886-903, 907, 911, 915, 919, 923, 927 |
| python/hsfs/core/opensearch/\_\_init\_\_.py                                      |        3 |        0 |    100% |           |
| python/hsfs/core/opensearch\_api/\_\_init\_\_.py                                 |        3 |        3 |      0% |       5-7 |
| python/hsfs/core/project\_api/\_\_init\_\_.py                                    |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/query\_constructor\_api.py                                      |       10 |        5 |     50% |     24-32 |
| python/hsfs/core/schema\_validation.py                                           |      157 |       11 |     93% |39, 106, 114, 167-175, 282 |
| python/hsfs/core/search\_api.py                                                  |        3 |        0 |    100% |           |
| python/hsfs/core/services\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/share\_api.py                                                   |       80 |       80 |      0% |    20-292 |
| python/hsfs/core/spine\_group\_engine.py                                         |       17 |       11 |     35% |     24-49 |
| python/hsfs/core/statistics\_api.py                                              |       79 |       63 |     20% |44-53, 84-110, 141-163, 183-187, 205-224, 237, 267-317 |
| python/hsfs/core/statistics\_engine.py                                           |      114 |       17 |     85% |129-156, 333-334, 481 |
| python/hsfs/core/storage\_connector\_api.py                                      |       56 |       39 |     30% |35-46, 60-69, 90-100, 121-126, 139, 166-176, 181-191, 198-210, 228-248, 267-287 |
| python/hsfs/core/tag\_schemas\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/tags\_api/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_api.py                                       |       58 |       39 |     33% |31-40, 53-62, 70-80, 87-93, 101-113, 131-142, 173-184, 206-218, 230-240 |
| python/hsfs/core/training\_dataset\_engine.py                                    |       56 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_job\_conf.py                                 |       37 |       14 |     62% |25-28, 32, 36, 40, 44, 48, 52, 56, 60, 63, 66 |
| python/hsfs/core/transformation\_execution\_dag.py                               |      179 |       25 |     86% |78, 200, 209-220, 262, 327-328, 346-360 |
| python/hsfs/core/transformation\_function\_api.py                                |       26 |       16 |     38% |42-51, 80-95, 108-118 |
| python/hsfs/core/transformation\_function\_engine.py                             |      504 |       89 |     82% |215-216, 224-228, 252, 283-287, 309, 314-327, 361-363, 384, 398, 789, 829-830, 839, 844-849, 856-862, 894, 896, 951-971, 984-1000, 1043-1053, 1108, 1203-1236, 1279-1286, 1397, 1509-1514, 1565, 1622-1627, 1660 |
| python/hsfs/core/type\_systems.py                                                |        2 |        0 |    100% |           |
| python/hsfs/core/util\_sql.py                                                    |       38 |       21 |     45% |37-74, 91-106 |
| python/hsfs/core/validation\_report\_api.py                                      |       34 |       21 |     38% |44-65, 75-87, 95-113, 123-140 |
| python/hsfs/core/validation\_report\_engine.py                                   |       41 |        5 |     88% |59, 84-86, 110 |
| python/hsfs/core/validation\_result\_api.py                                      |       12 |        4 |     67% |     50-64 |
| python/hsfs/core/validation\_result\_engine.py                                   |       33 |        3 |     91% |78, 82, 119 |
| python/hsfs/core/variable\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/vector\_db\_client.py                                           |      252 |       72 |     71% |80-83, 98, 123-127, 131, 172-195, 222, 227, 233, 257, 293-295, 327, 353-359, 368, 378, 395, 422-423, 436-455, 475-489, 492-499, 506, 512-521, 525, 529, 533-543, 547 |
| python/hsfs/core/vector\_server.py                                               |      675 |      502 |     26% |199-259, 266-268, 275-309, 321-336, 345-359, 369, 393-458, 496-578, 624-806, 849-919, 935-964, 983-1015, 1038-1079, 1115-1168, 1189-1193, 1221-1224, 1228-1230, 1232-1236, 1239-1241, 1245, 1247, 1249, 1251, 1266, 1268, 1273, 1280, 1284-1286, 1307-1328, 1355-1381, 1402-1428, 1436-1450, 1453-1473, 1482-1506, 1531-1576, 1581-1593, 1606-1651, 1682-1699, 1714-1727, 1759-1804, 1830-1878, 1900-1913, 1919, 1925, 1937-1941, 1945-1960, 1964-1972, 1976, 1980, 1984, 1988-1996, 2002, 2008, 2013, 2020-2034, 2040-2051, 2055-2059, 2063, 2067-2090, 2094-2114, 2119-2125, 2130-2136 |
| python/hsfs/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsfs/embedding.py                                                         |      155 |       40 |     74% |48, 61-63, 71, 79-100, 103, 106, 146-154, 171, 177, 183, 217, 225-232, 235, 314-318, 348-350, 354-355, 393, 401, 408 |
| python/hsfs/engine/\_\_init\_\_.py                                               |       42 |        6 |     86% |34, 41, 45, 49-50, 80 |
| python/hsfs/engine/python.py                                                     |      884 |      126 |     86% |309, 313, 336, 338-340, 344, 372, 378, 404, 410, 466-470, 492, 550, 591-602, 619, 645-649, 662-671, 709-712, 752-758, 795-804, 850-851, 896, 961-964, 994, 997-998, 1026-1027, 1053, 1072, 1088, 1163-1166, 1250, 1384-1388, 1497-1498, 1509-1510, 1525, 1574, 1639-1651, 1659, 1662, 1824, 1875, 1883, 1905, 1920-1924, 1954, 1982, 2012-2016, 2055, 2073-2076, 2107, 2119-2123, 2357-2359, 2365, 2371, 2698, 2702-2703, 2728-2732, 2734-2742, 2758-2759, 2765-2766, 2810, 2849-2850 |
| python/hsfs/engine/spark.py                                                      |      970 |      236 |     76% |88-93, 161-162, 179-180, 187-189, 201-207, 228, 236, 266-274, 301-311, 320, 367-375, 483, 492-496, 503-535, 573-583, 622, 641-642, 645, 698, 731-738, 831, 1035-1036, 1142, 1175-1179, 1199-1228, 1231-1248, 1251-1280, 1374, 1379, 1398-1438, 1464, 1504, 1567-1578, 1597, 1615-1657, 1734, 1748-1767, 1771-1778, 1833, 1835, 1837, 1871, 1881-1882, 1906-1907, 1919, 2001-2007, 2093-2103, 2327, 2357-2363, 2510, 2518-2532, 2536, 2652-2653, 2659-2660, 2666-2667, 2673-2674, 2687-2688, 2691, 2706 |
| python/hsfs/engine/spark\_metrics.py                                             |      115 |       26 |     77% |48-54, 78-79, 99-102, 107, 110, 128-129, 162, 185-194 |
| python/hsfs/engine/spark\_no\_metastore.py                                       |       14 |        5 |     64% | 35-44, 48 |
| python/hsfs/expectation\_suite.py                                                |      250 |       73 |     71% |53, 86, 101, 195, 241, 260, 271-276, 288, 295-310, 347, 381-385, 435-444, 474-487, 509-513, 520, 523-541, 551, 561-563, 575, 585, 595-597, 615-617, 642, 659, 663 |
| python/hsfs/feature.py                                                           |      174 |        9 |     95% |158, 198, 218, 234, 264, 294, 310, 348, 378 |
| python/hsfs/feature\_group.py                                                    |     1328 |      350 |     74% |352-357, 428, 640, 681, 709, 732, 758, 770, 786, 805, 822, 840-852, 867-879, 895, 911, 953-955, 975-977, 996-998, 1035-1036, 1064-1066, 1098-1099, 1139-1154, 1188-1191, 1223-1224, 1246-1264, 1300-1303, 1346-1364, 1399-1404, 1458, 1475, 1480, 1489, 1509-1511, 1545, 1578-1580, 1631-1652, 1704-1712, 1774-1779, 1797, 1804, 1855-1860, 1915-1920, 1983-1988, 2058-2063, 2085, 2101, 2128, 2132, 2154, 2179, 2194, 2207, 2223, 2248-2249, 2297-2298, 2336-2337, 2362-2369, 2394, 2418, 2461, 2479-2481, 2511, 2565, 2593, 2626, 2636, 2646, 2688-2689, 2696, 2708-2717, 2729, 2738, 2741-2769, 2778, 2784-2793, 2830, 2852, 2900-2901, 2929-2930, 3123, 3227, 3232-3237, 3298, 3458, 3463, 3467, 3498, 3501-3505, 3553, 3613-3622, 3649-3653, 3745, 3749, 3753, 3833, 3849, 4013, 4031, 4040, 4064, 4318-4338, 4385, 4404-4419, 4449, 4584-4586, 4610-4638, 4653, 4657, 4672, 4682, 4686-4687, 4697, 4701-4724, 4737, 4785, 4787, 4789, 4792, 4796, 4800, 4939, 4961, 4968-4976, 4992, 5002, 5008, 5012, 5019-5022, 5039-5052, 5058-5063, 5075, 5087-5094, 5235-5236, 5326-5356, 5385-5386, 5503, 5521, 5530, 5554-5558, 5589-5593, 5649-5658, 5670, 5677, 5685-5693, 5696, 5727, 5729, 5776, 5876-5877, 5905-5906, 5945, 5955-5958, 5970-5972, 5975-5979, 5982, 5985 |
| python/hsfs/feature\_group\_commit.py                                            |       84 |       16 |     81% |61-64, 67, 70, 113, 121, 125, 129, 133, 137, 141, 145, 149, 153 |
| python/hsfs/feature\_group\_writer.py                                            |       17 |        0 |    100% |           |
| python/hsfs/feature\_logger.py                                                   |       14 |       14 |      0% |     16-38 |
| python/hsfs/feature\_logger\_async.py                                            |      128 |      128 |      0% |    16-293 |
| python/hsfs/feature\_store.py                                                    |      338 |      115 |     66% |201, 220-225, 272, 274, 300, 329-346, 370, 407-408, 434-446, 468, 495, 523, 562-564, 593-595, 621-623, 641-643, 684, 709, 729, 949-950, 1172-1217, 1333-1372, 1550-1592, 1704-1722, 1816-1822, 1876, 1984, 2005, 2124-2141, 2215-2229, 2271, 2307-2309, 2313-2316, 2358-2364, 2385, 2409, 2440-2444, 2465, 2489, 2519, 2531, 2621, 2679, 2737, 2795, 2850 |
| python/hsfs/feature\_store\_activity.py                                          |       94 |       94 |      0% |    16-183 |
| python/hsfs/feature\_view.py                                                     |      812 |      260 |     68% |193, 287, 362-364, 393, 482-488, 497-498, 523, 528-531, 667, 829, 836, 999, 1006-1007, 1069, 1126, 1135-1156, 1220-1231, 1239-1251, 1399, 1451, 1478, 1502, 1519, 1539-1543, 1561-1566, 1587, 1613, 1623, 1828-1866, 2122-2166, 2410-2463, 2535-2545, 2862-2903, 2911-2912, 3084-3137, 3148-3154, 3308-3321, 3390-3407, 3432-3438, 3470, 3511, 3547, 3581, 3614, 3641-3643, 3667-3669, 3696-3698, 3722-3724, 3777-3785, 3795-3827, 3874-3879, 3935-3940, 3999-4004, 4070-4075, 4095, 4114, 4157, 4180, 4187, 4289-4307, 4340, 4381-4384, 4430-4432, 4441-4448, 4604-4625, 4677, 4727, 4752, 4767, 4791, 4811-4812, 4835-4844, 5017-5020, 5025-5038, 5044-5045, 5091, 5129, 5139, 5145, 5155, 5165, 5170, 5223, 5233, 5295, 5306, 5341-5356, 5387-5389, 5400, 5414, 5424, 5430-5432, 5548 |
| python/hsfs/ge\_expectation.py                                                   |      101 |       13 |     87% |40, 68, 120, 123, 126, 144, 158-161, 171, 186, 201 |
| python/hsfs/ge\_validation\_result.py                                            |      146 |       18 |     88% |52, 111, 157, 167, 182, 193, 199, 214, 233, 273, 286, 292, 295-301 |
| python/hsfs/hopsworks\_udf.py                                                    |      454 |       12 |     97% |143, 407-409, 637, 748, 754, 954, 969, 971, 1201, 1492 |
| python/hsfs/online\_config.py                                                    |       91 |        1 |     99% |       137 |
| python/hsfs/serving\_key.py                                                      |       60 |       10 |     83% |57, 94-98, 108, 113, 123, 128 |
| python/hsfs/split\_statistics.py                                                 |       28 |        2 |     93% |    57, 65 |
| python/hsfs/statistics.py                                                        |      115 |       27 |     77% |85, 87, 89, 102, 136-149, 152, 155, 158, 170, 174-182, 196, 202, 208, 214, 226 |
| python/hsfs/statistics\_config.py                                                |       60 |        4 |     93% |50, 109, 112, 115 |
| python/hsfs/storage\_connector.py                                                |     1800 |      414 |     77% |145, 168-175, 277, 316, 322, 336, 353-356, 368-379, 396-399, 411-423, 442-444, 469, 472, 474, 476, 491, 493-499, 508-511, 554, 578-580, 615-617, 622-635, 670, 676-677, 714, 872-897, 1079, 1112-1124, 1138, 1202, 1283, 1323, 1479, 1491, 1512, 1555-1569, 1578, 1580-1581, 1600-1614, 1655, 1676, 1825, 1884, 1903, 2018, 2090, 2137-2146, 2153, 2245-2254, 2510-2519, 2526, 2528, 2559-2560, 2633, 2677-2686, 2777, 2794, 2877, 3012-3013, 3098-3109, 3205, 3210, 3215, 3224-3246, 3262, 3392-3403, 3443-3452, 3461, 3466, 3471, 3476, 3481, 3486, 3491, 3494-3512, 3515, 3519-3538, 3549-3573, 3601, 3660, 3664, 3668, 3672, 3676, 3680, 3684, 3688, 3692, 3696, 3700, 3704, 3709, 3714, 3717-3738, 3741, 3992, 4017-4021, 4028-4036, 4044, 4059-4153, 4165-4170, 4204-4209, 4215, 4221, 4227, 4233, 4239, 4245, 4260-4262, 4281-4283, 4287-4305, 4329-4330, 4334, 4338, 4341, 4351-4352, 4357-4367, 4371, 4375, 4378, 4416-4424, 4427-4431, 4439, 4443, 4447, 4451, 4455, 4459, 4463, 4467, 4471, 4475, 4478-4496, 4534-4538, 4547, 4549-4556, 4560, 4564, 4568-4570, 4573-4584, 4587 |
| python/hsfs/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsfs/training\_dataset.py                                                 |      463 |      117 |     75% |202, 245, 250-267, 276, 285, 294, 320, 345, 350, 360, 387, 405, 415, 417, 419, 423, 443, 452, 463, 475, 484, 493, 502, 511, 520, 529, 538, 543, 547, 689-707, 748-754, 773-778, 783-804, 817, 833, 845, 860, 872, 887-888, 907-912, 918-928, 930, 939-947, 950-959, 969, 974, 1001, 1012, 1018, 1022, 1038, 1044, 1057, 1074, 1092-1094, 1112-1114, 1127, 1139, 1145-1153 |
| python/hsfs/training\_dataset\_feature.py                                        |       80 |        8 |     90% |59, 88, 134, 150, 160, 163-166 |
| python/hsfs/training\_dataset\_split.py                                          |       54 |        7 |     87% |52, 60, 68, 76, 84, 87, 90 |
| python/hsfs/transformation\_function.py                                          |      155 |       15 |     90% |138, 169, 231-233, 241, 548, 575, 585, 616, 641-645 |
| python/hsfs/transformation\_statistics.py                                        |      134 |       15 |     89% |94, 100, 108, 114, 120, 126, 146, 176, 190, 200, 206, 212, 224, 276, 283 |
| python/hsfs/usage.py                                                             |        2 |        0 |    100% |           |
| python/hsfs/user/\_\_init\_\_.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/util.py                                                              |       66 |       10 |     85% |75-76, 88, 214-228 |
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
| python/hsml/core/model\_api.py                                                   |       79 |       59 |     25% |38-48, 77-93, 115-143, 151-160, 175-188, 199-210, 224-234, 254-266, 287-311, 330-354 |
| python/hsml/core/model\_registry\_api.py                                         |       24 |       16 |     33% | 27, 38-62 |
| python/hsml/core/model\_serving\_api.py                                          |       53 |       20 |     62% |38-46, 51-63, 72-73, 120-127 |
| python/hsml/core/serving\_api.py                                                 |      139 |      105 |     24% |50-62, 74-84, 98-113, 121-124, 135-149, 158-166, 174-181, 194-202, 215-225, 243-249, 257-280, 292-312, 315-319, 327-330, 341-349, 360-365, 396-415 |
| python/hsml/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsml/deployable\_component.py                                             |       56 |        5 |     91% |73, 95, 105, 115, 125 |
| python/hsml/deployable\_component\_logs.py                                       |       57 |        6 |     89% |75, 110, 114, 119, 123, 126 |
| python/hsml/deployment.py                                                        |      327 |       57 |     83% |282, 369, 422, 517, 538-544, 547, 550, 568, 574, 584, 590, 600, 606, 614, 618, 624, 628, 634, 638, 648, 652, 658, 668, 674, 678, 684, 688, 694, 698, 708, 712, 718, 722, 728, 732, 738, 742, 748, 752, 768, 772, 778, 784, 790, 794, 800, 804, 820, 824, 830, 834, 840, 844, 847-852 |
| python/hsml/deployment\_tracing\_config.py                                       |       65 |        9 |     86% |54, 59, 95-97, 100, 116, 126, 131 |
| python/hsml/engine/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/engine/local\_engine.py                                              |       46 |       26 |     43% |35-36, 39, 42-59, 76-95, 98-100, 103-105, 108, 111-114 |
| python/hsml/engine/model\_engine.py                                              |      415 |      231 |     44% |81-101, 107-134, 144, 156-157, 167, 173-184, 218-247, 253-260, 273-293, 334-418, 421, 431-554, 607-609, 627, 632, 678-679, 684, 696, 699-700, 732-739, 750-753, 755, 761, 768-769, 785-786, 796-797, 829-831, 855-880, 883-900, 903-917, 920, 930, 939, 948, 956, 973, 990 |
| python/hsml/engine/serving\_engine.py                                            |      420 |      320 |     24% |76-113, 116-168, 171-209, 212-251, 254-274, 277-308, 311-314, 321-326, 329-330, 341-370, 376-383, 386-441, 444-471, 474-519, 522-528, 531-545, 548-555, 558-583, 678-680, 717, 772-804, 814-822, 831-893, 905-926, 941-945, 950-982 |
| python/hsml/inference\_batcher.py                                                |       77 |       13 |     83% |52, 83-85, 88, 93, 95, 97, 108, 118, 128, 138, 141 |
| python/hsml/inference\_endpoint.py                                               |       84 |        7 |     92% |54, 67, 109-111, 133, 155 |
| python/hsml/inference\_logger.py                                                 |       66 |        9 |     86% |51, 93-95, 98, 103, 114, 124, 127 |
| python/hsml/kafka\_topic/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/llm/\_\_init\_\_.py                                                  |        0 |        0 |    100% |           |
| python/hsml/llm/model.py                                                         |       13 |        6 |     54% |     70-75 |
| python/hsml/llm/predictor.py                                                     |       14 |        0 |    100% |           |
| python/hsml/llm/signature.py                                                     |       13 |        4 |     69% |     74-88 |
| python/hsml/model.py                                                             |      322 |       50 |     84% |139-144, 150-165, 297, 338, 428, 466, 559, 588, 603, 606, 618-622, 625, 628, 652, 662, 672, 682, 692, 702, 712, 716, 726, 736, 740, 746, 750, 762, 772, 784, 794, 804, 813, 822, 831, 837, 841, 846, 849 |
| python/hsml/model\_registry.py                                                   |      189 |       27 |     86% |79-80, 99-107, 130, 157-166, 172, 178, 184, 313, 318, 330, 357, 409, 424, 436, 442, 448, 454, 460, 463-468 |
| python/hsml/model\_schema.py                                                     |       20 |        5 |     75% |50, 57, 60-66 |
| python/hsml/model\_serving.py                                                    |      185 |       24 |     87% |40-41, 96, 123-125, 163-167, 170-178, 187, 352, 530, 638, 644, 650, 656, 659, 773 |
| python/hsml/predictor.py                                                         |      405 |       36 |     91% |50, 176, 264, 272-274, 362, 407, 454, 464, 474, 484-485, 495, 499, 506, 519-520, 536, 546, 560, 570, 580, 608-611, 621, 641, 647, 651, 661, 671, 681, 783-788 |
| python/hsml/predictor\_state.py                                                  |       87 |       14 |     84% |53, 83-103, 160 |
| python/hsml/predictor\_state\_condition.py                                       |       50 |        7 |     86% |42, 62-64, 67, 70, 97 |
| python/hsml/python/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/python/endpoint.py                                                   |       10 |        0 |    100% |           |
| python/hsml/python/model.py                                                      |       13 |        6 |     54% |     70-75 |
| python/hsml/python/predictor.py                                                  |        9 |        5 |     44% |     25-33 |
| python/hsml/python/signature.py                                                  |       13 |        4 |     69% |     74-88 |
| python/hsml/resources.py                                                         |      153 |       13 |     92% |51, 71, 84, 94, 104, 107, 149, 168, 203, 207, 237, 247, 250 |
| python/hsml/scaling\_config.py                                                   |      209 |       28 |     87% |104, 108-109, 211-213, 217, 244, 254-263, 275, 285, 295, 305, 315, 325, 335, 338, 365, 379, 406, 416, 421 |
| python/hsml/schema.py                                                            |       30 |        3 |     90% |72, 79, 82 |
| python/hsml/sklearn/\_\_init\_\_.py                                              |        0 |        0 |    100% |           |
| python/hsml/sklearn/model.py                                                     |       13 |        6 |     54% |     70-75 |
| python/hsml/sklearn/predictor.py                                                 |        7 |        3 |     57% |     25-28 |
| python/hsml/sklearn/signature.py                                                 |       13 |        4 |     69% |     74-88 |
| python/hsml/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsml/tensorflow/\_\_init\_\_.py                                           |        0 |        0 |    100% |           |
| python/hsml/tensorflow/model.py                                                  |       13 |        6 |     54% |     70-75 |
| python/hsml/tensorflow/predictor.py                                              |        9 |        5 |     44% |     25-33 |
| python/hsml/tensorflow/signature.py                                              |       13 |        4 |     69% |     74-88 |
| python/hsml/torch/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/torch/model.py                                                       |       13 |        6 |     54% |     70-75 |
| python/hsml/torch/predictor.py                                                   |        9 |        5 |     44% |     25-33 |
| python/hsml/torch/signature.py                                                   |       13 |        4 |     69% |     74-88 |
| python/hsml/transformer.py                                                       |       69 |        7 |     90% |33, 74, 124-127, 146 |
| python/hsml/util/\_\_init\_\_.py                                                 |       18 |       18 |      0% |      5-22 |
| python/hsml/utils/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/utils/schema/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hsml/utils/schema/column.py                                               |        7 |        0 |    100% |           |
| python/hsml/utils/schema/columnar\_schema.py                                     |       61 |        0 |    100% |           |
| python/hsml/utils/schema/tensor.py                                               |        8 |        0 |    100% |           |
| python/hsml/utils/schema/tensor\_schema.py                                       |       34 |        0 |    100% |           |
| python/hsml/version.py                                                           |        2 |        2 |      0% |     17-22 |
| **TOTAL**                                                                        | **37102** | **12126** | **67%** |           |


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