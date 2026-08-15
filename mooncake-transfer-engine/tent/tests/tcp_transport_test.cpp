// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/rpc/rpc.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/topology.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {
namespace {

TransferStatus WaitForTcpTask(TcpTransport& transport,
                              Transport::SubBatchRef batch, int task_id) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    TransferStatus status;
    do {
        auto get_status = transport.getTransferStatus(batch, task_id, status);
        if (!get_status.ok()) {
            ADD_FAILURE() << get_status.ToString();
            return status;
        }
        if (status.s != TransferStatusEnum::PENDING) return status;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);

    ADD_FAILURE() << "Timed out waiting for TCP task " << task_id;
    return status;
}

// ---------------------------------------------------------------------------
// TcpParams unit tests
// ---------------------------------------------------------------------------

TEST(TcpParamsTest, DefaultValues) {
    TcpParams params;
    EXPECT_EQ(params.max_retry_count, 3);
    EXPECT_EQ(params.retry_base_delay_ms, 100ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 2'000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 16);
}

// ---------------------------------------------------------------------------
// TcpTask unit tests — atomic semantics
// ---------------------------------------------------------------------------

TEST(TcpTaskTest, DefaultConstruction) {
    TcpTask task;
    EXPECT_EQ(task.status_word.load(), TransferStatusEnum::PENDING);
    EXPECT_EQ(task.transferred_bytes.load(), 0u);
    EXPECT_EQ(task.target_addr, 0u);
}

TEST(TcpTaskTest, AtomicStatusTransitions) {
    TcpTask task;
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::PENDING);

    task.status_word.store(TransferStatusEnum::COMPLETED,
                           std::memory_order_release);
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::COMPLETED);

    task.status_word.store(TransferStatusEnum::FAILED,
                           std::memory_order_release);
    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::FAILED);
}

TEST(TcpTaskTest, AtomicTransferredBytes) {
    TcpTask task;
    constexpr size_t kPayload = 1024 * 1024;
    task.transferred_bytes.store(kPayload, std::memory_order_release);
    EXPECT_EQ(task.transferred_bytes.load(std::memory_order_acquire), kPayload);
}

TEST(TcpTaskTest, MoveConstruction) {
    TcpTask a;
    a.status_word.store(TransferStatusEnum::COMPLETED,
                        std::memory_order_relaxed);
    a.transferred_bytes.store(4096, std::memory_order_relaxed);
    a.target_addr = 0xdeadbeef;

    TcpTask b(std::move(a));
    EXPECT_EQ(b.status_word.load(), TransferStatusEnum::COMPLETED);
    EXPECT_EQ(b.transferred_bytes.load(), 4096u);
    EXPECT_EQ(b.target_addr, 0xdeadbeef);
}

// ---------------------------------------------------------------------------
// TcpSubBatch unit tests
// ---------------------------------------------------------------------------

TEST(TcpSubBatchTest, EmptyBatch) {
    TcpSubBatch batch;
    batch.max_size = 64;
    EXPECT_EQ(batch.size(), 0u);
}

TEST(TcpSubBatchTest, EmplaceAndSize) {
    TcpSubBatch batch;
    batch.max_size = 64;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    batch.task_list.emplace_back();
    batch.task_list.emplace_back();
    EXPECT_EQ(batch.size(), 3u);
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // Verify that pointers taken after reserve remain valid when more
    // tasks are emplaced (important for async dispatch correctness).
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    // Add more tasks (within reserved capacity)
    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    // First pointer must still be valid
    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

// ---------------------------------------------------------------------------
// TcpTransport config loading test
// ---------------------------------------------------------------------------

TEST(TcpTransportConfigTest, ConfigOverridesDefaults) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);

    EXPECT_EQ(conf->get("transports/tcp/max_retry_count", 0), 5);
    EXPECT_EQ(conf->get("transports/tcp/retry_base_delay_ms", 0ULL), 200ULL);
    EXPECT_EQ(conf->get("transports/tcp/retry_max_delay_ms", 0ULL), 4000ULL);
    EXPECT_EQ(conf->get("transports/tcp/max_concurrent_tasks", 0), 32);
}

TEST(TcpTransportConfigTest, MissingConfigUsesDefaults) {
    auto conf = std::make_shared<Config>();

    TcpParams defaults;
    EXPECT_EQ(
        conf->get("transports/tcp/max_retry_count", defaults.max_retry_count),
        3);
    EXPECT_EQ(conf->get("transports/tcp/max_concurrent_tasks",
                        defaults.max_concurrent_tasks),
              16);
}

// ---------------------------------------------------------------------------
// Cross-thread visibility test (verifies atomic correctness)
// ---------------------------------------------------------------------------

TEST(TcpTaskTest, CrossThreadVisibility) {
    TcpTask task;
    std::atomic<bool> writer_done{false};

    std::thread writer([&]() {
        task.transferred_bytes.store(8192, std::memory_order_release);
        task.status_word.store(TransferStatusEnum::COMPLETED,
                               std::memory_order_release);
        writer_done.store(true, std::memory_order_release);
    });

    // Spin until writer signals done
    while (!writer_done.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    EXPECT_EQ(task.status_word.load(std::memory_order_acquire),
              TransferStatusEnum::COMPLETED);
    EXPECT_EQ(task.transferred_bytes.load(std::memory_order_acquire), 8192u);

    writer.join();
}

// ---------------------------------------------------------------------------
// Exponential backoff calculation test
// ---------------------------------------------------------------------------

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    // Simulate the backoff logic from doTransferWithRetry
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = std::min(delay * 2, cap);
    }

    // 100 → 200 → 400 → 800 → 1600 → 2000 (capped)
    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);  // capped at retry_max_delay_ms
}

// A retry after RpcServiceError is intended to re-resolve rpc_server_addr.
// This test keeps authoritative metadata and the two data endpoints in-process
// so the stale-cache sequence is deterministic:
//
//   1. A successful transfer primes the sole TCP worker's thread-local segment
//      cache with address A.
//   2. Authoritative metadata changes to B without an invalidation push.
//   3. A starts returning a service error while B succeeds.
//
// A correct retry must invalidate the cached descriptor, fetch metadata again,
// and reach B. The pre-fix implementation only clears its local output string;
// withCachedSegment then reuses the structurally-valid cached descriptor, so
// both attempts go to A and the metadata endpoint is never queried again.
TEST(TcpRetryTest, RpcServiceErrorRefreshesCachedRpcServerAddress) {
    CoroRpcAgent server_a;
    CoroRpcAgent server_b;
    CoroRpcAgent metadata_server;

    std::atomic<bool> fail_a{false};
    std::atomic<int> a_calls{0};
    std::atomic<int> b_calls{0};
    std::atomic<int> metadata_fetches{0};

    ASSERT_TRUE(
        server_a
            .registerFunction(
                SendData,
                [&](const std::string_view&, std::string& response) {
                    a_calls.fetch_add(1, std::memory_order_relaxed);
                    if (fail_a.load(std::memory_order_acquire)) {
                        response = "injected service failure at A";
                    }
                })
            .ok());
    ASSERT_TRUE(
        server_b
            .registerFunction(
                SendData,
                [&](const std::string_view&, std::string&) {
                    b_calls.fetch_add(1, std::memory_order_relaxed);
                })
            .ok());

    uint16_t port_a = 0;
    uint16_t port_b = 0;
    uint16_t metadata_port = 0;
    ASSERT_TRUE(server_a.start(port_a).ok());
    ASSERT_TRUE(server_b.start(port_b).ok());

    const std::string addr_a = "127.0.0.1:" + std::to_string(port_a);
    const std::string addr_b = "127.0.0.1:" + std::to_string(port_b);
    std::string authoritative_rpc_addr = addr_a;
    std::mutex authoritative_mu;

    constexpr uint64_t kRemoteAddr = 0x100000;
    constexpr size_t kLength = 8;
    ASSERT_TRUE(
        metadata_server
            .registerFunction(
                GetSegmentDesc,
                [&](const std::string_view&, std::string& response) {
                    metadata_fetches.fetch_add(1, std::memory_order_relaxed);
                    SegmentDesc desc;
                    desc.name = "stale-rpc-address-test";
                    desc.type = SegmentType::Memory;
                    desc.machine_id = "remote-machine";
                    {
                        std::lock_guard<std::mutex> lock(authoritative_mu);
                        desc.rpc_server_addr = authoritative_rpc_addr;
                    }
                    BufferDesc buffer;
                    buffer.addr = kRemoteAddr;
                    buffer.length = kLength;
                    buffer.location = "cpu:0";
                    std::get<MemorySegmentDesc>(desc.detail)
                        .buffers.push_back(buffer);
                    response = json(desc).dump();
                })
            .ok());
    ASSERT_TRUE(metadata_server.start(metadata_port).ok());
    const std::string metadata_addr =
        "127.0.0.1:" + std::to_string(metadata_port);

    auto metadata =
        std::make_shared<ControlService>("p2p", "", nullptr);
    SegmentID remote_segment = 0;
    ASSERT_TRUE(metadata->segmentManager()
                    .openRemote(remote_segment, metadata_addr)
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/tcp/max_concurrent_tasks", 1);
    config->set("transports/tcp/max_retry_count", 1);
    config->set("transports/tcp/retry_base_delay_ms", 1ULL);
    config->set("transports/tcp/retry_max_delay_ms", 1ULL);

    TcpTransport transport;
    std::string local_segment_name = "127.0.0.1:1";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata,
                             std::make_shared<Topology>(), config)
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());

    std::array<uint8_t, kLength> source{};
    Request request;
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = remote_segment;
    request.target_offset = kRemoteAddr;
    request.length = source.size();

    // Prime the sole worker's thread-local remote-segment cache with A.
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    ASSERT_EQ(WaitForTcpTask(transport, batch, 0).s,
              TransferStatusEnum::COMPLETED);
    ASSERT_EQ(metadata_fetches.load(std::memory_order_relaxed), 1);
    ASSERT_EQ(a_calls.load(std::memory_order_relaxed), 1);

    // Change only the authoritative descriptor. No SegmentUpdated RPC is sent,
    // so the worker cache remains at A until retry explicitly invalidates it.
    {
        std::lock_guard<std::mutex> lock(authoritative_mu);
        authoritative_rpc_addr = addr_b;
    }
    fail_a.store(true, std::memory_order_release);
    a_calls.store(0, std::memory_order_relaxed);

    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    const auto retried = WaitForTcpTask(transport, batch, 1);

    // These expectations describe the required behavior and intentionally fail
    // against the current implementation. Its observed counts are A=2, B=0,
    // metadata_fetches=1: clearing the local string did not evict the cached A.
    EXPECT_EQ(a_calls.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(b_calls.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(metadata_fetches.load(std::memory_order_relaxed), 2);
    EXPECT_EQ(retried.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(transport.freeSubBatch(batch).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
