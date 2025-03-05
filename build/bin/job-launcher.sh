#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#!/bin/bash

if [ $# -gt 3 ]; then
  echo "Supports up to three parameters!"
  exit 1
elif [ $# -ge 3 ]; then
# If a log path is provided,Will start in the background
    nohup ./pudding-client.jar -c $1 -j $2  > $3 2>&1 &
elif [ $# -ge 2 ]; then
   ./pudding-client.jar -c $1 -j $2
else
  echo "At least two parameters are required!"
  exit 1
fi
