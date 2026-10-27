// SPDX-License-Identifier: MIT
// pipeline — a small header-only C++17 library for building pipelined,
// multi-threaded processing chains (e.g. a radio receiver:
// demodulator -> decoder -> data link -> application).
//
//   #include <pipeline/pipeline.hpp>
//
// See docs/tutorial.html for a guided tour.
#pragma once

#include "block.hpp"
#include "channel.hpp"
#include "feeder.hpp"
#include "graph.hpp"
#include "json.hpp"
#include "parameters.hpp"
#include "pool.hpp"
#include "snapshot.hpp"
#include "stage.hpp"
#include "value.hpp"

#define PIPELINE_VERSION_MAJOR 1
#define PIPELINE_VERSION_MINOR 2
#define PIPELINE_VERSION_PATCH 0
