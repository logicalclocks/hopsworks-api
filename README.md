# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/logicalclocks/hopsworks-api/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                             |    Stmts |     Miss |   Cover |   Missing |
|--------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| python/hopsworks/\_\_init\_\_.py                                                 |      241 |       82 |     66% |62, 77-79, 83-85, 95-97, 149, 152, 155, 164-166, 274, 302, 306, 314, 339, 365-367, 394-399, 413, 443, 449, 459, 461-493, 504, 535-537, 580-597, 608-610, 621-623, 627-629, 633, 637 |
| python/hopsworks/alert/\_\_init\_\_.py                                           |        6 |        6 |      0% |      5-10 |
| python/hopsworks/alert\_receiver/\_\_init\_\_.py                                 |        6 |        6 |      0% |      5-10 |
| python/hopsworks/app/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/cli/\_\_init\_\_.py                                             |        0 |        0 |    100% |           |
| python/hopsworks/cli/\_\_main\_\_.py                                             |        3 |        3 |      0% |       3-7 |
| python/hopsworks/cli/auth.py                                                     |       39 |       24 |     38% |38, 90-119, 136 |
| python/hopsworks/cli/commands/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks/cli/commands/agent.py                                           |      162 |       37 |     77% |39-40, 70-85, 143-144, 170-171, 196-197, 230-231, 236-237, 240-241, 309-321, 331-332, 334-335, 355, 358-359, 372-373, 406 |
| python/hopsworks/cli/commands/app.py                                             |      116 |       15 |     87% |33-34, 62-63, 100, 158-159, 201-202, 232-233, 249, 256-257, 272 |
| python/hopsworks/cli/commands/context.py                                         |       73 |       23 |     68% |111, 123-130, 133-136, 139-144, 147-154, 190-191 |
| python/hopsworks/cli/commands/datasource.py                                      |      231 |       85 |     63% |58-59, 63-66, 78-93, 97, 181-193, 233-246, 274-284, 349-366, 383, 394-395, 413-414, 416-417, 433-447, 460-470, 501-502, 515-516, 519-520, 532, 547-548 |
| python/hopsworks/cli/commands/deployment.py                                      |      168 |       46 |     73% |35-36, 67-68, 72-85, 105, 109, 164, 168-169, 171, 180-181, 212-213, 238-239, 268, 272-273, 278-279, 282-283, 355-371, 381-382, 384-385, 405, 408-409, 418-419, 421 |
| python/hopsworks/cli/commands/env.py                                             |       55 |       55 |      0% |    12-168 |
| python/hopsworks/cli/commands/fg.py                                              |      369 |      156 |     58% |65-66, 103-104, 107-108, 161, 163, 165, 170, 252-284, 374-423, 473-482, 488-489, 493, 495, 538-577, 605-606, 632-633, 637-646, 688-689, 692-695, 717-718, 721-722, 748-749, 772-773, 781-783, 795-811, 823-843, 865, 874-882, 889, 893-896 |
| python/hopsworks/cli/commands/files.py                                           |      103 |       31 |     70% |43-44, 77-78, 115-116, 141-148, 181-190, 214-221, 237, 241-242 |
| python/hopsworks/cli/commands/fv.py                                              |      205 |       86 |     58% |62-63, 66-67, 80-90, 106-118, 122, 192-193, 199-200, 203-204, 228-229, 259-260, 265-266, 271, 309-335, 363-364, 367, 373-374, 384-388, 398, 409, 418-438, 442-450 |
| python/hopsworks/cli/commands/init.py                                            |       66 |        3 |     95% |59, 98, 143 |
| python/hopsworks/cli/commands/job.py                                             |      238 |      102 |     57% |28, 31-38, 48, 50-54, 74-75, 104-105, 109-120, 124-137, 182-195, 242, 251-252, 261, 310-331, 344-352, 374, 377-380, 384-385, 388-389, 518-519, 522, 535-549, 565-566, 582-589, 596-597, 599, 606-607, 613, 622-625 |
| python/hopsworks/cli/commands/login.py                                           |       26 |       26 |      0% |      8-67 |
| python/hopsworks/cli/commands/model.py                                           |      154 |       57 |     63% |66-69, 72, 75-76, 90-106, 111, 117, 205, 210-213, 217-221, 236-237, 270-271, 289-296, 305-312, 317, 322, 326-327 |
| python/hopsworks/cli/commands/project.py                                         |       52 |       52 |      0% |     3-100 |
| python/hopsworks/cli/commands/search.py                                          |       56 |       56 |      0% |    10-160 |
| python/hopsworks/cli/commands/setup.py                                           |      159 |       53 |     67% |53-60, 81, 85-91, 113, 151-169, 204, 305, 315-321, 333-334, 340-341, 354, 376-377, 411-412, 427-433 |
| python/hopsworks/cli/commands/superset.py                                        |      138 |       38 |     72% |51-52, 91-101, 116-122, 141-142, 155, 199-200, 217, 220-221, 255, 278-279, 295-301, 328-330, 336-337, 345 |
| python/hopsworks/cli/commands/td.py                                              |      122 |       51 |     58% |37-38, 44-45, 116-117, 125, 168-196, 223-237, 244-245, 255, 259-260, 267-275 |
| python/hopsworks/cli/commands/transformation.py                                  |       83 |       13 |     84% |36-37, 83, 88-89, 98-99, 156, 163-164, 167, 174, 177 |
| python/hopsworks/cli/commands/trino.py                                           |      131 |      131 |      0% |    13-315 |
| python/hopsworks/cli/commands/update.py                                          |       49 |       15 |     69% |54, 69-70, 73, 76, 87-96, 107-108 |
| python/hopsworks/cli/config.py                                                   |      141 |       22 |     84% |64, 78, 94, 98-99, 108-110, 129, 133, 138-140, 157-158, 212, 214, 281-287 |
| python/hopsworks/cli/joinspec.py                                                 |       18 |        0 |    100% |           |
| python/hopsworks/cli/main.py                                                     |       52 |        6 |     88% |55-56, 64, 126, 133, 187 |
| python/hopsworks/cli/output.py                                                   |       55 |        4 |     93% |120, 132, 144, 155 |
| python/hopsworks/cli/session.py                                                  |       35 |       14 |     60% |33-34, 52, 70, 72-84, 98, 100-102 |
| python/hopsworks/cli/templates/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/client/\_\_init\_\_.py                                          |       15 |        0 |    100% |           |
| python/hopsworks/client/auth/\_\_init\_\_.py                                     |        4 |        4 |      0% |       5-8 |
| python/hopsworks/client/base/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/exceptions/\_\_init\_\_.py                               |       18 |        0 |    100% |           |
| python/hopsworks/client/external/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/client/hopsworks/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/command/\_\_init\_\_.py                                         |        2 |        2 |      0% |       5-6 |
| python/hopsworks/connection/\_\_init\_\_.py                                      |        2 |        0 |    100% |           |
| python/hopsworks/constants.py                                                    |        2 |        2 |      0% |     17-45 |
| python/hopsworks/core/\_\_init\_\_.py                                            |        5 |        0 |    100% |           |
| python/hopsworks/core/alerts\_api/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/app\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/chart\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/dashboard\_api/\_\_init\_\_.py                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/dataset\_api/\_\_init\_\_.py                               |        3 |        3 |      0% |       5-7 |
| python/hopsworks/core/env\_var\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/environment\_api/\_\_init\_\_.py                           |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/execution\_api/\_\_init\_\_.py                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/flink\_cluster\_api/\_\_init\_\_.py                        |        2 |        2 |      0% |       5-6 |
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
| python/hopsworks/core/project\_api/\_\_init\_\_.py                               |        2 |        0 |    100% |           |
| python/hopsworks/core/rest\_endpoint/\_\_init\_\_.py                             |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/search\_api/\_\_init\_\_.py                                |       12 |       12 |      0% |      5-16 |
| python/hopsworks/core/secret\_api/\_\_init\_\_.py                                |        2 |        0 |    100% |           |
| python/hopsworks/core/services\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/superset\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tag\_schemas\_api/\_\_init\_\_.py                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/tags\_api/\_\_init\_\_.py                                  |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/trino\_api/\_\_init\_\_.py                                 |        2 |        2 |      0% |       5-6 |
| python/hopsworks/core/variable\_api/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/decorators/\_\_init\_\_.py                                      |        8 |        0 |    100% |           |
| python/hopsworks/engine/\_\_init\_\_.py                                          |        0 |        0 |    100% |           |
| python/hopsworks/engine/environment\_engine/\_\_init\_\_.py                      |        2 |        2 |      0% |       5-6 |
| python/hopsworks/engine/execution\_engine/\_\_init\_\_.py                        |        2 |        2 |      0% |       5-6 |
| python/hopsworks/engine/git\_engine/\_\_init\_\_.py                              |        2 |        2 |      0% |       5-6 |
| python/hopsworks/env\_var/\_\_init\_\_.py                                        |        2 |        2 |      0% |       5-6 |
| python/hopsworks/environment/\_\_init\_\_.py                                     |        2 |        2 |      0% |       5-6 |
| python/hopsworks/execution/\_\_init\_\_.py                                       |        2 |        2 |      0% |       5-6 |
| python/hopsworks/flink\_cluster/\_\_init\_\_.py                                  |        2 |        2 |      0% |       5-6 |
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
| python/hopsworks/mcp/\_\_init\_\_.py                                             |        3 |        3 |      0% |       1-3 |
| python/hopsworks/mcp/models/\_\_init\_\_.py                                      |        0 |        0 |    100% |           |
| python/hopsworks/mcp/models/dataset.py                                           |       25 |       25 |      0% |     16-57 |
| python/hopsworks/mcp/models/feature\_group.py                                    |       20 |       20 |      0% |     16-41 |
| python/hopsworks/mcp/models/job.py                                               |       35 |       35 |      0% |     16-76 |
| python/hopsworks/mcp/models/project.py                                           |        3 |        3 |      0% |     17-30 |
| python/hopsworks/mcp/prompts/\_\_init\_\_.py                                     |        2 |        2 |      0% |     18-19 |
| python/hopsworks/mcp/prompts/project.py                                          |       26 |       26 |      0% |     18-95 |
| python/hopsworks/mcp/prompts/system.py                                           |       12 |       12 |      0% |     18-51 |
| python/hopsworks/mcp/resources/\_\_init\_\_.py                                   |        0 |        0 |    100% |           |
| python/hopsworks/mcp/resources/project.py                                        |       28 |       28 |      0% |    17-113 |
| python/hopsworks/mcp/run\_server.py                                              |       56 |       56 |      0% |    16-176 |
| python/hopsworks/mcp/server.py                                                   |       21 |       21 |      0% |     19-57 |
| python/hopsworks/mcp/tools/\_\_init\_\_.py                                       |        7 |        7 |      0% |     18-24 |
| python/hopsworks/mcp/tools/auth.py                                               |       15 |       15 |      0% |     18-95 |
| python/hopsworks/mcp/tools/brewer.py                                             |       79 |       79 |      0% |    17-171 |
| python/hopsworks/mcp/tools/dataset.py                                            |       72 |       72 |      0% |    16-297 |
| python/hopsworks/mcp/tools/feature\_group.py                                     |       67 |       67 |      0% |    16-195 |
| python/hopsworks/mcp/tools/jobs.py                                               |       25 |       25 |      0% |     16-89 |
| python/hopsworks/mcp/tools/project.py                                            |       58 |       58 |      0% |    16-208 |
| python/hopsworks/mcp/tools/terminal.py                                           |       46 |       46 |      0% |    16-140 |
| python/hopsworks/mcp/utils/\_\_init\_\_.py                                       |        0 |        0 |    100% |           |
| python/hopsworks/mcp/utils/auth.py                                               |       11 |       11 |      0% |     16-76 |
| python/hopsworks/mcp/utils/tags.py                                               |       16 |       16 |      0% |      1-20 |
| python/hopsworks/project/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hopsworks/secret/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hopsworks/spark.py                                                        |       17 |       17 |      0% |     18-95 |
| python/hopsworks/tag/\_\_init\_\_.py                                             |        2 |        2 |      0% |       5-6 |
| python/hopsworks/triggered\_alert/\_\_init\_\_.py                                |        2 |        2 |      0% |       5-6 |
| python/hopsworks/user/\_\_init\_\_.py                                            |        2 |        2 |      0% |       5-6 |
| python/hopsworks/util/\_\_init\_\_.py                                            |       30 |       30 |      0% |      5-34 |
| python/hopsworks/version.py                                                      |        2 |        2 |      0% |     17-22 |
| python/hopsworks\_common/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hopsworks\_common/alert.py                                                |      132 |       58 |     56% |37-42, 46-51, 56, 61, 66, 71, 76, 81, 84, 92, 101, 104, 124-135, 140, 145, 150, 154, 166, 185-195, 200, 205, 209, 220, 240-251, 256, 261, 266, 270, 282, 303-315, 320, 325, 330, 335, 339, 352 |
| python/hopsworks\_common/alert\_receiver.py                                      |      207 |       94 |     55% |32-37, 40, 48, 69, 74, 77, 81, 87, 90, 100-105, 108, 116, 137, 142, 145, 149, 155, 158, 169-177, 180, 184, 199-201, 205-208, 213, 218, 221, 225, 232, 235, 245-250, 253, 257, 278, 283, 286, 290, 296, 299, 346-347, 357, 362, 367, 372, 375, 379-394, 397, 400-408, 411 |
| python/hopsworks\_common/alert\_route.py                                         |       50 |       12 |     76% |50-51, 56, 61, 66, 71, 76, 86, 89, 93, 104, 107 |
| python/hopsworks\_common/app.py                                                  |      162 |       10 |     94% |103, 112, 233-236, 280-281, 323, 329 |
| python/hopsworks\_common/client/\_\_init\_\_.py                                  |       67 |       23 |     66% |43-59, 65-67, 74, 77, 83, 92, 98, 107, 113, 120, 129, 135, 143, 149, 157 |
| python/hopsworks\_common/client/auth.py                                          |       36 |       14 |     61% |39-40, 52, 55-56, 71-72, 77-83 |
| python/hopsworks\_common/client/base.py                                          |      120 |       52 |     57% |70-75, 83-88, 92, 96, 100-101, 112, 115-116, 155, 183, 187, 212, 217-231, 235-241, 245-251, 255-262, 274-283 |
| python/hopsworks\_common/client/exceptions.py                                    |      131 |        8 |     94% |48-50, 56, 161-165, 173, 185 |
| python/hopsworks\_common/client/external.py                                      |      205 |       86 |     58% |63-106, 109-203, 265-288, 386-387, 395-399, 426, 429, 433, 437 |
| python/hopsworks\_common/client/hopsworks.py                                     |      105 |       63 |     40% |62-88, 92-102, 106, 109, 112, 115, 123-128, 136-141, 144-152, 155-159, 167-174, 185, 188, 192 |
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
| python/hopsworks\_common/constants.py                                            |      181 |        2 |     99% |    25, 28 |
| python/hopsworks\_common/core/\_\_init\_\_.py                                    |        0 |        0 |    100% |           |
| python/hopsworks\_common/core/alerts\_api.py                                     |      262 |      170 |     35% |130-133, 165-168, 197-200, 231-241, 275-286, 325-337, 373-386, 427-441, 483-515, 559-592, 638-663, 704-728, 756-760, 794-799, 842-907, 931-935, 978-1010, 1043-1047, 1059-1062, 1070-1115, 1127-1131, 1134-1147 |
| python/hopsworks\_common/core/app\_api.py                                        |       65 |       38 |     42% |44-50, 66-72, 115-143, 152-167, 171-182, 204-211 |
| python/hopsworks\_common/core/chart\_api.py                                      |       31 |       14 |     55% |47, 56-61, 73, 102-113, 128, 137 |
| python/hopsworks\_common/core/constants.py                                       |       30 |        0 |    100% |           |
| python/hopsworks\_common/core/dashboard\_api.py                                  |       41 |       20 |     51% |41, 51-56, 68, 80, 96, 105, 121-126, 141-143 |
| python/hopsworks\_common/core/dataset.py                                         |       31 |       14 |     55% |33-37, 41-44, 48, 52, 56, 60, 64 |
| python/hopsworks\_common/core/dataset\_api.py                                    |      359 |      247 |     31% |106-168, 217-288, 300-366, 388, 405-408, 411, 422-426, 455, 470-471, 484, 497-499, 512, 556-580, 611-629, 655-664, 692-710, 736-754, 772-795, 827-846, 867-884, 900-915, 931-935, 964-1016, 1035, 1059, 1082-1094, 1107-1117, 1133-1149 |
| python/hopsworks\_common/core/env\_var\_api.py                                   |       50 |        0 |    100% |           |
| python/hopsworks\_common/core/environment\_api.py                                |       41 |       22 |     46% |63-86, 108-113, 146-151, 163-173 |
| python/hopsworks\_common/core/execution\_api.py                                  |       53 |       18 |     66% |65, 89-100, 105-111, 119-128, 131-141, 153-155 |
| python/hopsworks\_common/core/flink\_cluster\_api.py                             |       93 |       63 |     32% |43, 76-88, 91-107, 136-147, 176-179, 208-211, 237-240, 268-277, 306-312, 338-357, 393-422, 464-476 |
| python/hopsworks\_common/core/git\_api.py                                        |      177 |      130 |     27% |87-122, 135-142, 157, 176, 208-211, 228-254, 257-265, 268-291, 294-316, 321-344, 347-380, 383-410, 413-440, 443-470, 473-499, 502-529, 532-556, 559-572, 578-584 |
| python/hopsworks\_common/core/git\_op\_execution\_api.py                         |        9 |        4 |     56% |     24-36 |
| python/hopsworks\_common/core/git\_provider\_api.py                              |       45 |       31 |     31% |31-34, 39-44, 49-67, 70-81, 88-96 |
| python/hopsworks\_common/core/git\_remote\_api.py                                |       35 |       25 |     29% |25, 28-43, 46-61, 64-88, 91-113 |
| python/hopsworks\_common/core/hosts\_api.py                                      |       10 |        3 |     70% |     28-32 |
| python/hopsworks\_common/core/ingestion\_job.py                                  |       19 |        0 |    100% |           |
| python/hopsworks\_common/core/ingestion\_job\_conf.py                            |       39 |       14 |     64% |33-36, 40, 44, 48, 52, 56, 60, 64, 68, 71, 74 |
| python/hopsworks\_common/core/inode.py                                           |       41 |        6 |     85% |51, 55, 59, 67, 71, 75 |
| python/hopsworks\_common/core/job\_api.py                                        |       99 |       54 |     45% |78-91, 108-116, 131-138, 155-156, 175-185, 193-200, 212-219, 227-232, 239-242, 257-261, 287-293, 304-309, 317-320 |
| python/hopsworks\_common/core/job\_configuration.py                              |       21 |        1 |     95% |        75 |
| python/hopsworks\_common/core/kafka\_api.py                                      |       98 |       56 |     43% |70-82, 128-149, 165-170, 183-186, 196-204, 213-223, 236-243, 259-275, 292-296, 305-316, 321-330, 340, 369-389 |
| python/hopsworks\_common/core/library\_api.py                                    |       15 |        4 |     73% |     41-54 |
| python/hopsworks\_common/core/opensearch.py                                      |      240 |       75 |     69% |44, 52-98, 126, 161, 194, 205-208, 212-218, 223-230, 286-288, 297, 374-409, 452-455, 460-472, 513 |
| python/hopsworks\_common/core/opensearch\_api.py                                 |       54 |       15 |     72% |62-73, 86-87, 113-114, 135-140 |
| python/hopsworks\_common/core/project\_api.py                                    |       58 |       32 |     45% |39-43, 54-64, 75-79, 90-94, 108-115, 167-173, 176-179 |
| python/hopsworks\_common/core/rest.py                                            |       21 |        1 |     95% |        65 |
| python/hopsworks\_common/core/rest\_endpoint.py                                  |      234 |       59 |     75% |48-51, 60, 63, 68, 100, 104-107, 113, 122, 128, 139, 142-144, 150, 155, 165, 180-181, 184, 199-201, 204, 269-275, 278, 303-308, 311, 323, 331, 364, 380, 383, 408-411, 416, 427-431, 438, 448 |
| python/hopsworks\_common/core/search\_api.py                                     |      133 |       82 |     38% |45-47, 53, 59, 65, 69, 72, 84-103, 110, 115, 119, 122, 134-138, 237, 299-308, 362-371, 425-434, 485-494, 510-525, 543-562, 576-624 |
| python/hopsworks\_common/core/secret\_api.py                                     |       51 |       31 |     39% |48-53, 74-90, 110-116, 145-169, 177-183 |
| python/hopsworks\_common/core/services\_api.py                                   |       10 |        3 |     70% |     30-35 |
| python/hopsworks\_common/core/sink\_job\_configuration.py                        |      306 |       52 |     83% |67, 95-96, 99, 106-107, 114, 118, 122, 126, 150, 184-191, 193, 201, 247, 283, 292, 344-356, 365-373, 378, 447, 458, 466, 474, 482, 490, 498, 522, 531, 540, 550, 562, 573 |
| python/hopsworks\_common/core/superset\_api.py                                   |      156 |       45 |     71% |110-111, 210, 240-252, 265, 275, 290, 303, 337-350, 363, 373, 388, 401, 433-447, 460, 470, 485, 500 |
| python/hopsworks\_common/core/tag\_schemas\_api.py                               |       46 |       46 |      0% |    23-175 |
| python/hopsworks\_common/core/tags\_api.py                                       |       39 |       20 |     49% |62-68, 86-91, 113-119, 128-149 |
| python/hopsworks\_common/core/trino\_api.py                                      |       95 |        1 |     99% |       219 |
| python/hopsworks\_common/core/type\_systems.py                                   |      258 |       43 |     83% |319, 372, 394-396, 402-424, 464, 485, 489-513, 520 |
| python/hopsworks\_common/core/variable\_api.py                                   |       50 |       26 |     48% |74-82, 96-101, 112, 150-159, 169-170, 178-182 |
| python/hopsworks\_common/decorators.py                                           |       87 |        5 |     94% |129, 155, 165, 175, 185 |
| python/hopsworks\_common/engine/\_\_init\_\_.py                                  |        0 |        0 |    100% |           |
| python/hopsworks\_common/engine/alerts\_engine.py                                |       64 |        2 |     97% |   45, 101 |
| python/hopsworks\_common/engine/environment\_engine.py                           |       38 |       27 |     29% |27-31, 34-38, 41-45, 48-74, 77-90 |
| python/hopsworks\_common/engine/execution\_engine.py                             |      108 |       62 |     43% |57-80, 83-102, 119-144, 168, 181-195, 205-211, 218, 238 |
| python/hopsworks\_common/engine/git\_engine.py                                   |       21 |        8 |     62% |     49-62 |
| python/hopsworks\_common/env\_var.py                                             |       50 |        7 |     86% |73, 79, 87, 98, 106, 109, 117 |
| python/hopsworks\_common/environment.py                                          |       72 |       24 |     67% |63-66, 71, 81, 112-128, 159-175, 221, 224 |
| python/hopsworks\_common/execution.py                                            |      140 |       23 |     84% |74, 80-82, 94, 124, 142, 148, 154, 160, 166, 172, 188, 193, 240, 253, 266, 285, 288, 291, 296-304 |
| python/hopsworks\_common/flink\_cluster.py                                       |      121 |       66 |     45% |36-41, 44-62, 65-71, 96-122, 147, 178, 207, 234, 262, 305, 350, 375-378, 386, 392, 398, 404, 410, 416-419, 424-426 |
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
| python/hopsworks\_common/project.py                                              |      160 |       53 |     67% |104-107, 113, 119, 125, 131, 137, 143, 187, 208, 229, 238-241, 250-253, 262, 280, 284, 293, 302, 311, 320, 329, 338, 347-349, 358-360, 369-371, 380-382, 394, 406, 436, 472, 477, 480, 483-485, 490-491 |
| python/hopsworks\_common/search\_results.py                                      |      221 |       47 |     79% |58, 61, 110, 116, 121, 144, 154-170, 197, 209, 215, 232, 240, 250-259, 281-282, 304-305, 327-328, 395, 401, 407, 413, 445, 461 |
| python/hopsworks\_common/secret.py                                               |       61 |       25 |     59% |49-55, 59-62, 68, 74, 80, 86, 92, 98, 110, 113, 116, 119-121, 126-127 |
| python/hopsworks\_common/spark\_connect\_utils.py                                |       38 |        6 |     84% |66-67, 103-104, 110-111 |
| python/hopsworks\_common/tag.py                                                  |       80 |       16 |     80% |60-63, 69, 88, 91-94, 109, 120, 141, 153, 156, 159 |
| python/hopsworks\_common/triggered\_alert.py                                     |       78 |       39 |     50% |26-28, 32-35, 38, 41, 48, 51, 71-79, 83-88, 93, 98, 103, 108, 113, 118, 123, 128, 133, 136, 139, 152, 155 |
| python/hopsworks\_common/usage.py                                                |      179 |       25 |     86% |51-52, 79-80, 104, 135-139, 166, 171, 175, 180-182, 186-187, 192-194, 226, 232, 250-251 |
| python/hopsworks\_common/user.py                                                 |       42 |        5 |     88% |54-55, 60, 65, 68 |
| python/hopsworks\_common/util.py                                                 |      530 |      136 |     74% |67-68, 80-102, 105-108, 168-171, 275, 317, 322, 433, 453, 496, 500-503, 509-526, 532-538, 543-545, 565, 593, 619, 731, 737-739, 747-752, 797, 806-819, 856-861, 866, 870, 875, 879, 884, 912-920, 924-952, 956-958, 962-979, 991-998, 1003, 1008, 1013 |
| python/hopsworks\_common/version.py                                              |        1 |        0 |    100% |           |
| python/hsfs/\_\_init\_\_.py                                                      |       18 |        2 |     89% |    52, 56 |
| python/hsfs/builtin\_transformations.py                                          |      223 |      179 |     20% |33-37, 42-48, 66-80, 85-88, 113-122, 139-140, 170-200, 220-260, 278-319, 341-369, 388-409, 429-463, 493-523, 542-544, 560-563, 588-592, 610-615, 644-646 |
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
| python/hsfs/constructor/fs\_query.py                                             |       63 |       16 |     75% |70, 96, 102, 108, 112, 116, 122-128, 139-140, 151-152 |
| python/hsfs/constructor/hudi\_feature\_group\_alias.py                           |       26 |        0 |    100% |           |
| python/hsfs/constructor/join.py                                                  |       39 |        2 |     95% |    56, 84 |
| python/hsfs/constructor/prepared\_statement\_parameter.py                        |       35 |        8 |     77% |46-48, 51, 54, 57, 69, 73 |
| python/hsfs/constructor/query.py                                                 |      336 |       68 |     80% |108-110, 319, 324, 331, 348-350, 378-383, 413, 612-618, 681-686, 710, 713, 741, 768-786, 804-808, 831-833, 839, 842, 844, 847, 850-856, 920, 948-951, 1055-1062, 1097-1098, 1100, 1109-1110, 1121 |
| python/hsfs/constructor/serving\_prepared\_statement.py                          |       65 |       19 |     71% |61-63, 66, 69, 76-82, 86, 90, 96, 100, 104, 108, 112, 136, 140 |
| python/hsfs/core/\_\_init\_\_.py                                                 |        0 |        0 |    100% |           |
| python/hsfs/core/arrow\_flight\_client.py                                        |      312 |      152 |     51% |30, 71-75, 79, 83, 89, 95, 103, 109, 116, 161, 171, 212-213, 228-236, 243-262, 270-276, 281-295, 302, 307, 342-346, 354-361, 371-374, 380-387, 390-393, 396-399, 402-404, 407-409, 417-426, 432-454, 467-469, 482-498, 503-505, 528-529, 554-580, 588, 592, 597, 601, 615, 620 |
| python/hsfs/core/chart.py                                                        |       88 |       39 |     56% |45-53, 57-64, 67, 80, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 128, 132, 136, 140, 144, 148, 152, 160-162, 172-174 |
| python/hsfs/core/chart\_api.py                                                   |       24 |       15 |     38% |24-31, 39-46, 55-63, 72-80, 88-96 |
| python/hsfs/core/constants.py                                                    |        2 |        0 |    100% |           |
| python/hsfs/core/dashboard.py                                                    |       46 |       21 |     54% |39-41, 45-52, 55, 62, 66, 70, 74, 78, 82, 86, 94-96, 106-108 |
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
| python/hsfs/core/explicit\_provenance.py                                         |      228 |      152 |     33% |35, 38, 41, 44, 57-63, 69, 75, 81, 84, 87, 98, 101, 109-123, 137-140, 149, 159, 175, 184, 187, 206, 209, 216-233, 237-243, 247-260, 264-281, 285-302, 306-351, 371-433, 438-485 |
| python/hsfs/core/external\_feature\_group\_engine.py                             |       57 |       18 |     68% |36, 82, 96-133, 204-206 |
| python/hsfs/core/feature\_descriptive\_statistics.py                             |      179 |       24 |     87% |103, 106-107, 123-124, 136, 172, 178, 211, 214, 217, 247, 253, 259, 265, 271, 277, 283, 295, 301, 311, 325, 335, 341 |
| python/hsfs/core/feature\_group\_api.py                                          |      141 |       87 |     38% |51-63, 77-89, 159-165, 173-175, 192-214, 230-250, 263-273, 288-297, 326-337, 361-372, 397-412, 430-442, 460-472, 495-512, 533-550, 574-591, 615-632 |
| python/hsfs/core/feature\_group\_base\_engine.py                                 |       77 |       19 |     75% |76, 90, 107-110, 127-130, 147-150, 167-170, 214 |
| python/hsfs/core/feature\_group\_engine.py                                       |      236 |       33 |     86% |124, 201-214, 247-248, 361, 373-380, 490-492, 505-507, 520-521, 561-562, 634, 644, 683, 700, 733-740, 791, 798 |
| python/hsfs/core/feature\_logging.py                                             |       74 |       34 |     54% |29, 58-61, 65-81, 89-91, 96, 101, 106, 109-120, 125, 128, 136, 139 |
| python/hsfs/core/feature\_logging\_client.py                                     |       52 |       52 |      0% |    16-109 |
| python/hsfs/core/feature\_monitoring\_config.py                                  |      278 |       92 |     67% |55, 59, 63-65, 72-74, 77, 80, 160-215, 218, 221, 224, 318-329, 361-365, 398-402, 444-451, 470-485, 505, 531-536, 559-565, 586-591, 610, 631, 634-646, 681-686, 703, 709, 715, 721, 752, 754, 756, 770, 772, 794, 808, 822, 826, 840, 850, 858, 878, 883, 893, 915, 926 |
| python/hsfs/core/feature\_monitoring\_config\_api.py                             |       72 |       47 |     35% |63-70, 86-95, 108-114, 128-134, 150-156, 176-181, 191-197, 213-219, 233-240, 262-290 |
| python/hsfs/core/feature\_monitoring\_config\_engine.py                          |      103 |       53 |     49% |142, 145, 148, 151, 160, 178, 180, 184, 190, 192, 199-202, 219-224, 241-246, 255, 284-308, 322-324, 338, 354-383, 422, 470-474 |
| python/hsfs/core/feature\_monitoring\_result.py                                  |      117 |       28 |     76% |101-125, 128, 131, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206, 212, 218, 224 |
| python/hsfs/core/feature\_monitoring\_result\_api.py                             |       44 |       25 |     43% |62-69, 82-88, 108-116, 132-138, 154-173 |
| python/hsfs/core/feature\_monitoring\_result\_engine.py                          |      120 |       19 |     84% |215-235, 371, 428, 435, 464, 496 |
| python/hsfs/core/feature\_store\_activity\_api.py                                |       12 |       12 |      0% |     16-73 |
| python/hsfs/core/feature\_store\_api.py                                          |        8 |        3 |     62% |     32-34 |
| python/hsfs/core/feature\_view\_api.py                                           |      169 |       97 |     43% |71-72, 82-83, 105-122, 139-155, 160-166, 173-175, 180-182, 228, 258-271, 281-283, 292-293, 300-301, 312-316, 323-324, 329-330, 333-334, 339-343, 348-357, 381-395, 422-436, 449-458, 465-473, 480-488, 495-510, 520-527, 537-549 |
| python/hsfs/core/feature\_view\_engine.py                                        |      521 |      167 |     68% |103-105, 153-161, 170-178, 204-205, 273, 381-387, 389-394, 396-403, 425-432, 603-621, 625, 629, 647-648, 712-716, 773, 789, 798, 895, 907-913, 916-919, 968-981, 1005, 1007, 1034-1035, 1049, 1062-1067, 1092, 1116-1121, 1140-1147, 1155-1156, 1227-1230, 1264-1267, 1336, 1391-1407, 1410, 1413-1416, 1515-1610, 1870, 1874, 1892-1937, 1943-1947, 1950-1974, 1977-1983, 1992-1995, 1998, 2001, 2006-2020, 2023-2024 |
| python/hsfs/core/great\_expectation\_engine.py                                   |       43 |        4 |     91% |75, 95-100 |
| python/hsfs/core/hosts\_api/\_\_init\_\_.py                                      |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/hudi\_engine.py                                                 |      123 |       11 |     91% |152, 218-223, 241-245, 254-258 |
| python/hsfs/core/inferred\_metadata.py                                           |       65 |       10 |     85% |54-56, 80, 88, 96, 118, 138, 165, 172 |
| python/hsfs/core/ingestion\_job/\_\_init\_\_.py                                  |        2 |        0 |    100% |           |
| python/hsfs/core/ingestion\_job\_conf/\_\_init\_\_.py                            |        2 |        0 |    100% |           |
| python/hsfs/core/inode/\_\_init\_\_.py                                           |        2 |        0 |    100% |           |
| python/hsfs/core/job/\_\_init\_\_.py                                             |        2 |        0 |    100% |           |
| python/hsfs/core/job\_api/\_\_init\_\_.py                                        |        2 |        0 |    100% |           |
| python/hsfs/core/job\_configuration/\_\_init\_\_.py                              |        2 |        0 |    100% |           |
| python/hsfs/core/job\_schedule/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_api/\_\_init\_\_.py                                      |        2 |        0 |    100% |           |
| python/hsfs/core/kafka\_engine.py                                                |      143 |       12 |     92% |244, 260, 266, 281, 319-330 |
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
| python/hsfs/core/schema\_validation.py                                           |      156 |       11 |     93% |39, 102, 110, 158-166, 273 |
| python/hsfs/core/search\_api.py                                                  |        3 |        0 |    100% |           |
| python/hsfs/core/services\_api/\_\_init\_\_.py                                   |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/share\_api.py                                                   |       80 |       80 |      0% |    20-292 |
| python/hsfs/core/spine\_group\_engine.py                                         |       17 |       11 |     35% |     24-47 |
| python/hsfs/core/statistics\_api.py                                              |       79 |       63 |     20% |44-53, 84-110, 141-163, 183-187, 205-224, 237, 267-317 |
| python/hsfs/core/statistics\_engine.py                                           |      109 |       18 |     83% |129-156, 397-403, 422 |
| python/hsfs/core/storage\_connector\_api.py                                      |       41 |       28 |     32% |34-45, 59-64, 77, 87-97, 104-116, 134-154, 173-193 |
| python/hsfs/core/tag\_schemas\_api/\_\_init\_\_.py                               |        2 |        2 |      0% |       5-6 |
| python/hsfs/core/tags\_api/\_\_init\_\_.py                                       |        2 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_api.py                                       |       58 |       39 |     33% |31-40, 53-62, 70-80, 87-93, 101-113, 131-142, 173-184, 206-218, 230-240 |
| python/hsfs/core/training\_dataset\_engine.py                                    |       56 |        0 |    100% |           |
| python/hsfs/core/training\_dataset\_job\_conf.py                                 |       37 |       14 |     62% |25-28, 32, 36, 40, 44, 48, 52, 56, 60, 63, 66 |
| python/hsfs/core/transformation\_function\_api.py                                |       26 |       16 |     38% |42-51, 80-95, 108-118 |
| python/hsfs/core/transformation\_function\_engine.py                             |      137 |       21 |     85% |158, 335, 433-466, 498, 501-525, 558 |
| python/hsfs/core/type\_systems.py                                                |        2 |        0 |    100% |           |
| python/hsfs/core/util\_sql.py                                                    |       38 |       21 |     45% |37-74, 91-106 |
| python/hsfs/core/validation\_report\_api.py                                      |       34 |       21 |     38% |44-65, 75-87, 95-113, 123-140 |
| python/hsfs/core/validation\_report\_engine.py                                   |       41 |        5 |     88% |59, 84-86, 110 |
| python/hsfs/core/validation\_result\_api.py                                      |       12 |        4 |     67% |     50-64 |
| python/hsfs/core/validation\_result\_engine.py                                   |       33 |        3 |     91% |78, 82, 119 |
| python/hsfs/core/variable\_api/\_\_init\_\_.py                                   |        2 |        0 |    100% |           |
| python/hsfs/core/vector\_db\_client.py                                           |      252 |       84 |     67% |80-83, 98, 122-200, 222, 227, 233, 257, 293-295, 327, 353-359, 368, 378, 395, 422-423, 436-455, 475-489, 492-499, 506, 512-521, 525, 529, 533-543, 547 |
| python/hsfs/core/vector\_server.py                                               |      666 |      521 |     22% |190-250, 257-259, 266-296, 306-321, 330-344, 353-364, 378-433, 469-550, 594-775, 816-885, 901-930, 949-981, 1000-1037, 1069-1121, 1142-1146, 1171-1225, 1245-1266, 1293-1319, 1340-1366, 1374-1388, 1391-1411, 1420-1444, 1467-1510, 1515-1527, 1540-1585, 1616-1633, 1648-1661, 1693-1738, 1764-1812, 1834-1847, 1853, 1859, 1863, 1867, 1871-1875, 1879-1894, 1898-1906, 1910, 1914, 1918, 1922-1930, 1936, 1942, 1947, 1954-1968, 1974-1985, 1989-1993, 1997, 2001-2024, 2028-2048, 2053-2059, 2064-2070 |
| python/hsfs/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsfs/embedding.py                                                         |      158 |       43 |     73% |48, 61-63, 71, 79-100, 103, 106, 146-154, 173, 179, 185, 191, 197, 212, 225, 233-240, 243, 322-326, 356-358, 362-363, 401, 409, 416 |
| python/hsfs/engine/\_\_init\_\_.py                                               |       42 |        6 |     86% |34, 41, 45, 49-50, 80 |
| python/hsfs/engine/python.py                                                     |      923 |      125 |     86% |308, 312, 335, 337-339, 343, 371, 377, 403, 409, 465-469, 491, 549, 590-601, 618, 644-653, 691-694, 734-740, 777-786, 832-833, 878, 943-946, 976, 979-980, 1008-1009, 1054, 1073, 1137, 1156, 1172, 1236-1239, 1320, 1443-1447, 1571-1572, 1583-1584, 1599, 1648, 1713-1725, 1736, 1862, 1902, 1910, 1930, 1945-1949, 1979, 2007, 2037-2041, 2078, 2096-2099, 2130, 2142-2146, 2380-2382, 2388, 2394, 2721, 2725-2726, 2751-2755, 2757-2765, 2781-2782, 2788-2789, 2833, 2872-2873 |
| python/hsfs/engine/spark.py                                                      |      929 |      190 |     80% |89-94, 161-162, 179-180, 187-189, 201-207, 228, 236, 266-274, 292, 301, 444, 453-457, 464-496, 534-541, 599, 660, 694, 747, 780-787, 869, 1071-1072, 1166, 1182, 1215-1219, 1239-1268, 1271-1288, 1291-1320, 1420, 1455, 1495, 1556-1567, 1586, 1604-1646, 1723, 1737-1756, 1760-1767, 1822, 1824, 1826, 1868-1869, 1893-1894, 1956, 2207, 2237-2243, 2390, 2398-2412, 2416, 2532-2533, 2539-2540, 2546-2547, 2553-2554, 2567-2568, 2571, 2586 |
| python/hsfs/engine/spark\_metrics.py                                             |      115 |       26 |     77% |48-54, 78-79, 99-102, 107, 110, 128-129, 162, 185-194 |
| python/hsfs/engine/spark\_no\_metastore.py                                       |       14 |        5 |     64% | 35-44, 48 |
| python/hsfs/expectation\_suite.py                                                |      250 |       73 |     71% |53, 86, 101, 195, 241, 260, 271-276, 288, 295-310, 347, 381-385, 435-444, 474-487, 509-513, 520, 523-541, 551, 561-563, 575, 585, 595-597, 615-617, 642, 659, 663 |
| python/hsfs/feature.py                                                           |      166 |        8 |     95% |155, 205, 221, 251, 281, 297, 335, 365 |
| python/hsfs/feature\_group.py                                                    |     1281 |      344 |     73% |312-317, 386, 598, 639, 667, 690, 716, 728, 744, 762, 779, 796-808, 823-835, 851, 867, 909-911, 931-933, 952-954, 991-992, 1020-1022, 1054-1055, 1095-1110, 1144-1147, 1179-1180, 1202-1220, 1256-1259, 1302-1320, 1355-1360, 1414, 1431, 1436, 1445, 1465-1467, 1501, 1534-1536, 1587-1608, 1660-1668, 1730-1735, 1753, 1760, 1811-1816, 1871-1876, 1939-1944, 2014-2019, 2041, 2057, 2084, 2088, 2110, 2135, 2150, 2163, 2178, 2203-2204, 2252-2253, 2291-2292, 2317-2324, 2349, 2373, 2416, 2434-2436, 2466, 2520, 2547, 2580, 2590, 2600, 2642-2643, 2650, 2662-2671, 2683, 2692, 2695-2723, 2732, 2738-2747, 2784, 2806, 2854-2855, 2883-2884, 3077, 3166, 3171-3176, 3217, 3369, 3374, 3378, 3409, 3412-3416, 3464, 3524-3533, 3560-3564, 3650, 3654, 3658, 3732, 3746, 3904, 3920, 3929, 3952, 4199-4217, 4264, 4283-4289, 4319, 4454-4456, 4480-4508, 4523, 4527, 4542, 4552, 4556-4557, 4567, 4571-4594, 4607, 4655, 4657, 4659, 4662, 4666, 4670, 4799, 4821, 4828-4836, 4852, 4862, 4868, 4872, 4879, 4885-4898, 4904-4909, 4919, 4930-4937, 5078-5079, 5169-5197, 5302, 5313, 5337-5341, 5372-5376, 5432-5441, 5453, 5460, 5468-5476, 5479, 5510, 5512, 5559, 5659-5660, 5688-5689, 5728, 5738-5741, 5753-5755, 5758-5762, 5765, 5768 |
| python/hsfs/feature\_group\_commit.py                                            |       84 |       16 |     81% |61-64, 67, 70, 113, 121, 125, 129, 133, 137, 141, 145, 149, 153 |
| python/hsfs/feature\_group\_writer.py                                            |       15 |        0 |    100% |           |
| python/hsfs/feature\_logger.py                                                   |       14 |       14 |      0% |     16-38 |
| python/hsfs/feature\_logger\_async.py                                            |      128 |      128 |      0% |    16-293 |
| python/hsfs/feature\_store.py                                                    |      304 |      105 |     65% |236, 238, 264, 293-310, 334, 371-372, 398-410, 432, 459, 487, 518-520, 546-548, 566-568, 609, 634, 653, 870-871, 1092-1137, 1253-1292, 1470-1512, 1624-1642, 1736-1742, 1796, 1904, 1923, 2040-2057, 2129-2143, 2185, 2221-2223, 2227-2230, 2271-2277, 2297, 2320, 2350-2354, 2374, 2397, 2427, 2439, 2528, 2585, 2642, 2699, 2753 |
| python/hsfs/feature\_store\_activity.py                                          |       94 |       94 |      0% |    16-183 |
| python/hsfs/feature\_view.py                                                     |      742 |      272 |     63% |190, 261, 336-338, 367, 450-504, 547-548, 599, 753-759, 913-921, 979-981, 1036-1038, 1046-1067, 1131-1142, 1150-1162, 1167, 1298, 1345, 1372, 1396, 1413, 1433-1437, 1455-1460, 1481, 1507, 1517, 1716-1753, 2003-2046, 2284-2336, 2408-2418, 2711-2750, 2758-2759, 2920-2971, 2982-2988, 3131-3143, 3207-3223, 3248-3254, 3286, 3327, 3363, 3397, 3430, 3457-3459, 3483-3485, 3512-3514, 3538-3540, 3587-3592, 3648-3653, 3712-3717, 3783-3788, 3808, 3827, 3870, 3893, 3900, 3955-3973, 4000, 4035-4038, 4083-4085, 4094-4101, 4257-4278, 4330, 4380, 4405, 4420, 4444, 4464-4465, 4488-4497, 4658-4661, 4666-4679, 4685-4686, 4710, 4718-4733, 4770, 4780, 4786, 4796, 4806, 4811, 4864, 4874, 4928, 4939, 4948, 4959-4967, 4998-5000, 5011, 5025, 5035, 5041-5043, 5046, 5053-5054, 5058-5069, 5074, 5159 |
| python/hsfs/ge\_expectation.py                                                   |      101 |       13 |     87% |40, 68, 120, 123, 126, 144, 158-161, 171, 186, 201 |
| python/hsfs/ge\_validation\_result.py                                            |      146 |       18 |     88% |52, 111, 157, 167, 182, 193, 199, 214, 233, 273, 286, 292, 295-301 |
| python/hsfs/hopsworks\_udf.py                                                    |      388 |       10 |     97% |142, 382-384, 598, 713, 719, 859, 1088, 1360 |
| python/hsfs/online\_config.py                                                    |       52 |        1 |     98% |        86 |
| python/hsfs/serving\_key.py                                                      |       60 |       10 |     83% |57, 94-98, 108, 113, 123, 128 |
| python/hsfs/split\_statistics.py                                                 |       28 |        2 |     93% |    57, 65 |
| python/hsfs/statistics.py                                                        |      115 |       27 |     77% |85, 87, 89, 102, 136-149, 152, 155, 158, 170, 174-182, 196, 202, 208, 214, 226 |
| python/hsfs/statistics\_config.py                                                |       60 |        5 |     92% |50, 53, 109, 112, 115 |
| python/hsfs/storage\_connector.py                                                |     1531 |      292 |     81% |125, 148-155, 207, 246, 252, 265, 282-285, 297-308, 325-328, 340-352, 371-373, 398, 401, 403, 405, 420, 422-428, 437-440, 483, 507-509, 544-546, 551-564, 589, 595-596, 633, 771-796, 978, 986-998, 1012, 1076, 1157, 1178, 1334, 1346, 1367, 1385-1399, 1408, 1410-1411, 1430-1444, 1485, 1505, 1633, 1692, 1711, 1826, 1878, 1927-1943, 1950, 2032-2041, 2274-2283, 2290, 2292, 2323-2324, 2397, 2441-2450, 2541, 2545, 2628, 2744-2745, 2830-2841, 2937, 2942, 2947, 2965, 3103-3112, 3121, 3126, 3131, 3136, 3141, 3146, 3151, 3154, 3158-3177, 3188-3210, 3238, 3297, 3301, 3305, 3309, 3313, 3317, 3321, 3325, 3329, 3333, 3337, 3341, 3346, 3351, 3354, 3450-3451, 3455, 3459, 3462, 3472-3473, 3478-3488, 3492, 3496, 3499, 3537-3545, 3548-3552, 3560, 3564, 3568, 3572, 3576, 3580, 3584, 3588, 3592, 3596, 3599-3617, 3655-3659, 3668, 3670-3677, 3681, 3685, 3689-3691, 3694-3707, 3710 |
| python/hsfs/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsfs/training\_dataset.py                                                 |      457 |      124 |     73% |201, 244, 249-266, 275, 284, 293, 319, 344, 349, 359, 376, 386, 404, 414, 416, 418, 422, 442, 451, 462, 474, 483, 492, 501, 510, 519, 528, 537, 542, 546, 678-696, 737-743, 762-767, 772-793, 806, 822, 834, 849, 861, 876-877, 896-901, 907-917, 919, 928-936, 939-948, 958, 963, 966-989, 999, 1005, 1009, 1025, 1031, 1044, 1061, 1079-1081, 1099-1101, 1114, 1126, 1132-1140 |
| python/hsfs/training\_dataset\_feature.py                                        |       80 |        8 |     90% |59, 88, 134, 150, 160, 163-166 |
| python/hsfs/training\_dataset\_split.py                                          |       54 |        7 |     87% |52, 60, 68, 76, 84, 87, 90 |
| python/hsfs/transformation\_function.py                                          |      150 |       16 |     89% |134, 165, 227-229, 237, 293, 541, 568, 578, 609, 634-638 |
| python/hsfs/transformation\_statistics.py                                        |      133 |       15 |     89% |94, 100, 108, 114, 120, 126, 146, 176, 190, 200, 206, 212, 224, 275, 282 |
| python/hsfs/usage.py                                                             |        2 |        0 |    100% |           |
| python/hsfs/user/\_\_init\_\_.py                                                 |        2 |        0 |    100% |           |
| python/hsfs/util.py                                                              |       66 |       10 |     85% |75-76, 88, 214-228 |
| python/hsfs/validation\_report.py                                                |      138 |       16 |     88% |72, 104, 148, 164, 174, 227, 244, 263, 281-287, 293, 296 |
| python/hsfs/version.py                                                           |        2 |        0 |    100% |           |
| python/hsml/\_\_init\_\_.py                                                      |       10 |        0 |    100% |           |
| python/hsml/client/\_\_init\_\_.py                                               |        2 |        0 |    100% |           |
| python/hsml/client/auth/\_\_init\_\_.py                                          |        5 |        5 |      0% |       5-9 |
| python/hsml/client/base/\_\_init\_\_.py                                          |        2 |        2 |      0% |       5-6 |
| python/hsml/client/exceptions/\_\_init\_\_.py                                    |       21 |        0 |    100% |           |
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
| python/hsml/core/explicit\_provenance.py                                         |      187 |       86 |     54% |65, 71, 77, 80, 87, 95-109, 138, 176, 179, 195, 198, 206-207, 227-264, 268-297, 313-334, 342-357, 362-387 |
| python/hsml/core/hdfs\_api.py                                                    |       21 |       12 |     43% |28, 52-76, 88 |
| python/hsml/core/huggingface\_api.py                                             |       28 |       18 |     36% |37-38, 75-86, 103-104, 119-121 |
| python/hsml/core/model\_api.py                                                   |       79 |       59 |     25% |38-48, 77-93, 115-143, 151-160, 175-188, 199-210, 224-234, 254-266, 287-311, 330-354 |
| python/hsml/core/model\_registry\_api.py                                         |       24 |       24 |      0% |     17-62 |
| python/hsml/core/model\_serving\_api.py                                          |       53 |       20 |     62% |38-46, 51-63, 72-73, 120-127 |
| python/hsml/core/serving\_api.py                                                 |      139 |      105 |     24% |50-62, 74-84, 98-113, 121-124, 135-149, 158-166, 174-181, 194-202, 215-225, 243-249, 257-280, 292-312, 315-319, 327-330, 341-349, 360-365, 396-415 |
| python/hsml/decorators/\_\_init\_\_.py                                           |        8 |        0 |    100% |           |
| python/hsml/deployable\_component.py                                             |       56 |        5 |     91% |73, 95, 105, 115, 125 |
| python/hsml/deployable\_component\_logs.py                                       |       57 |        6 |     89% |75, 110, 114, 119, 123, 126 |
| python/hsml/deployment.py                                                        |      320 |       57 |     82% |281, 366, 419, 514, 535-541, 544, 547, 565, 571, 581, 587, 597, 603, 611, 615, 621, 625, 631, 635, 645, 649, 655, 665, 671, 675, 681, 685, 691, 695, 705, 709, 715, 719, 725, 729, 735, 739, 745, 749, 755, 759, 765, 771, 777, 781, 787, 791, 807, 811, 817, 821, 827, 831, 834-839 |
| python/hsml/engine/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/engine/local\_engine.py                                              |       46 |       26 |     43% |35-36, 39, 42-59, 76-95, 98-100, 103-105, 108, 111-114 |
| python/hsml/engine/model\_engine.py                                              |      277 |      207 |     25% |45-65, 68-88, 98, 110-111, 121, 127-138, 172-201, 207-214, 227-247, 288-372, 375, 385-508, 511-545, 548-565, 568-582, 585, 595, 604, 613, 621, 638, 655 |
| python/hsml/engine/serving\_engine.py                                            |      416 |      316 |     24% |76-113, 116-168, 171-209, 212-251, 254-274, 277-308, 311-314, 321-326, 329-330, 341-370, 376-383, 386-441, 444-471, 474-519, 522-528, 531-545, 548-555, 558-583, 678-680, 717, 772-804, 814-822, 831-882, 894-915, 930-934, 939-971 |
| python/hsml/inference\_batcher.py                                                |       77 |       13 |     83% |52, 83-85, 88, 93, 95, 97, 108, 118, 128, 138, 141 |
| python/hsml/inference\_endpoint.py                                               |       84 |        7 |     92% |54, 67, 109-111, 133, 155 |
| python/hsml/inference\_logger.py                                                 |       66 |        9 |     86% |51, 93-95, 98, 103, 114, 124, 127 |
| python/hsml/kafka\_topic/\_\_init\_\_.py                                         |        2 |        0 |    100% |           |
| python/hsml/llm/\_\_init\_\_.py                                                  |        0 |        0 |    100% |           |
| python/hsml/llm/model.py                                                         |       13 |        6 |     54% |     70-75 |
| python/hsml/llm/predictor.py                                                     |       14 |        0 |    100% |           |
| python/hsml/llm/signature.py                                                     |       11 |        4 |     64% |     71-85 |
| python/hsml/model.py                                                             |      276 |       48 |     83% |138-143, 149-164, 276, 314, 407, 436, 451, 454, 466-470, 473, 476, 500, 510, 520, 530, 540, 550, 560, 564, 574, 584, 588, 594, 598, 610, 620, 632, 642, 652, 661, 670, 679, 685, 689, 694, 697 |
| python/hsml/model\_registry.py                                                   |      189 |       27 |     86% |79-80, 99-107, 130, 157-166, 172, 178, 184, 313, 318, 330, 357, 409, 424, 436, 442, 448, 454, 460, 463-468 |
| python/hsml/model\_schema.py                                                     |       20 |        5 |     75% |50, 57, 60-66 |
| python/hsml/model\_serving.py                                                    |      185 |       28 |     85% |40-41, 95, 122-124, 162-166, 169-177, 186, 257-260, 348, 403, 521, 628, 634, 640, 646, 649, 763 |
| python/hsml/predictor.py                                                         |      391 |       36 |     91% |49, 173, 260, 268-270, 353, 398, 443, 453, 463, 473-474, 484, 488, 495, 508-509, 525, 535, 549, 559, 569, 587-590, 600, 619, 625, 629, 639, 649, 659, 761-766 |
| python/hsml/predictor\_state.py                                                  |       87 |       14 |     84% |53, 81-101, 158 |
| python/hsml/predictor\_state\_condition.py                                       |       50 |        7 |     86% |42, 62-64, 67, 70, 97 |
| python/hsml/python/\_\_init\_\_.py                                               |        0 |        0 |    100% |           |
| python/hsml/python/endpoint.py                                                   |       10 |        0 |    100% |           |
| python/hsml/python/model.py                                                      |       13 |        6 |     54% |     70-75 |
| python/hsml/python/predictor.py                                                  |        9 |        5 |     44% |     25-33 |
| python/hsml/python/signature.py                                                  |       11 |        4 |     64% |     71-85 |
| python/hsml/resources.py                                                         |      153 |       13 |     92% |51, 71, 84, 94, 104, 107, 149, 168, 203, 207, 237, 247, 250 |
| python/hsml/scaling\_config.py                                                   |      212 |       28 |     87% |105, 109-110, 212-214, 218, 245, 255-264, 276, 286, 296, 306, 316, 326, 336, 339, 367, 381, 409, 419, 424 |
| python/hsml/schema.py                                                            |       30 |        3 |     90% |72, 79, 82 |
| python/hsml/sklearn/\_\_init\_\_.py                                              |        0 |        0 |    100% |           |
| python/hsml/sklearn/model.py                                                     |       13 |        6 |     54% |     70-75 |
| python/hsml/sklearn/predictor.py                                                 |        7 |        3 |     57% |     25-28 |
| python/hsml/sklearn/signature.py                                                 |       11 |        4 |     64% |     71-85 |
| python/hsml/tag/\_\_init\_\_.py                                                  |        2 |        0 |    100% |           |
| python/hsml/tensorflow/\_\_init\_\_.py                                           |        0 |        0 |    100% |           |
| python/hsml/tensorflow/model.py                                                  |       13 |        6 |     54% |     70-75 |
| python/hsml/tensorflow/predictor.py                                              |        9 |        5 |     44% |     25-33 |
| python/hsml/tensorflow/signature.py                                              |       11 |        4 |     64% |     71-85 |
| python/hsml/torch/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/torch/model.py                                                       |       13 |        6 |     54% |     70-75 |
| python/hsml/torch/predictor.py                                                   |        9 |        5 |     44% |     25-33 |
| python/hsml/torch/signature.py                                                   |       11 |        4 |     64% |     71-85 |
| python/hsml/transformer.py                                                       |       69 |        7 |     90% |33, 74, 124-127, 146 |
| python/hsml/util/\_\_init\_\_.py                                                 |       18 |       18 |      0% |      5-22 |
| python/hsml/utils/\_\_init\_\_.py                                                |        0 |        0 |    100% |           |
| python/hsml/utils/schema/\_\_init\_\_.py                                         |        0 |        0 |    100% |           |
| python/hsml/utils/schema/column.py                                               |        7 |        0 |    100% |           |
| python/hsml/utils/schema/columnar\_schema.py                                     |       61 |        0 |    100% |           |
| python/hsml/utils/schema/tensor.py                                               |        8 |        0 |    100% |           |
| python/hsml/utils/schema/tensor\_schema.py                                       |       34 |        0 |    100% |           |
| python/hsml/version.py                                                           |        2 |        2 |      0% |     17-22 |
| **TOTAL**                                                                        | **34162** | **12194** | **64%** |           |


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