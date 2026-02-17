#!/bin/bash

# Copyright 2020 University of California, Riverside
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file loops over static resources in the HTML directory and generates a version for each one
# based on the most recent git version in which the file was changed.
# This is useful for avoiding prolonged client-side cache
resources=()
for res in "${resources[@]}"; do
    revision=$(git log -n 1 --pretty=format:%H -- ucrstar.com/"${res}")
    echo "${res}?v=${revision:32}"
done
