// SPDX-License-Identifier: Apache-2.0
// Thin opaque-handle wrapper around the Splash CXLMemSim client.

#include "pgas/cxlmemsim_client.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#define LEGOMEM_MAX_IO_LANES 16
#define LEGOMEM_MAX_BULK_BYTES (64ULL * 1024ULL * 1024ULL)
#define LEGOMEM_MIB (1024ULL * 1024ULL)

typedef struct {
    cxlmemsim_ctx_t ctx[LEGOMEM_MAX_IO_LANES];
    int num_lanes;
    uint64_t node_stride_bytes;
    uint64_t interleave_bytes;
    uint32_t num_nodes;
} legomem_client_t;

typedef struct {
    cxlmemsim_ctx_t *ctx;
    uint64_t address;
    void *data;
    size_t size;
    int is_write;
    int status;
    uint64_t node_stride_bytes;
    uint64_t interleave_bytes;
    uint32_t num_nodes;
} legomem_io_t;

static uint64_t legomem_map_address(const legomem_io_t *io,
                                    uint64_t logical_address) {
    if (io->interleave_bytes == 0 || io->num_nodes < 2)
        return logical_address;

    uint64_t logical_stripe = logical_address / io->interleave_bytes;
    uint64_t stripe_offset = logical_address % io->interleave_bytes;
    uint64_t node = logical_stripe % io->num_nodes;
    uint64_t node_stripe = logical_stripe / io->num_nodes;
    return node * io->node_stride_bytes +
           node_stripe * io->interleave_bytes + stripe_offset;
}

static void *legomem_io_worker(void *opaque) {
    legomem_io_t *io = opaque;
    uint64_t address = io->address;
    uint8_t *data = io->data;
    size_t remaining = io->size;
    io->status = 0;
    while (remaining > 0) {
        size_t chunk = remaining;
        if (chunk > LEGOMEM_MAX_BULK_BYTES)
            chunk = LEGOMEM_MAX_BULK_BYTES;
        if (io->interleave_bytes > 0 && io->num_nodes > 1) {
            uint64_t offset = address % io->interleave_bytes;
            uint64_t to_boundary = io->interleave_bytes - offset;
            if (chunk > to_boundary)
                chunk = (size_t)to_boundary;
        } else if (io->node_stride_bytes > 0) {
            uint64_t offset = address % io->node_stride_bytes;
            uint64_t to_boundary = io->node_stride_bytes - offset;
            if (chunk > to_boundary)
                chunk = (size_t)to_boundary;
        }
        uint64_t physical_address = legomem_map_address(io, address);
        if (io->is_write) {
            io->status = cxlmemsim_remote_store(io->ctx, physical_address,
                                                data, chunk);
        } else {
            io->status = cxlmemsim_remote_load(io->ctx, physical_address,
                                               data, chunk);
        }
        if (io->status != 0)
            break;
        address += chunk;
        data += chunk;
        remaining -= chunk;
    }
    return NULL;
}

static int legomem_transfer(legomem_client_t *client, uint64_t address,
                            void *data, size_t size, int is_write) {
    size_t num_lines = (size + CXLMEMSIM_CACHELINE_SIZE - 1) /
                       CXLMEMSIM_CACHELINE_SIZE;
    int lanes = client->num_lanes;
    if ((size_t)lanes > num_lines)
        lanes = (int)num_lines;

    pthread_t threads[LEGOMEM_MAX_IO_LANES];
    legomem_io_t ios[LEGOMEM_MAX_IO_LANES];
    int started[LEGOMEM_MAX_IO_LANES] = {0};

    for (int lane = 0; lane < lanes; ++lane) {
        size_t first_line = num_lines * (size_t)lane / (size_t)lanes;
        size_t last_line = num_lines * (size_t)(lane + 1) / (size_t)lanes;
        size_t offset = first_line * CXLMEMSIM_CACHELINE_SIZE;
        size_t end = last_line * CXLMEMSIM_CACHELINE_SIZE;
        if (end > size)
            end = size;
        ios[lane] = (legomem_io_t){
            .ctx = &client->ctx[lane],
            .address = address + offset,
            .data = (uint8_t *)data + offset,
            .size = end - offset,
            .is_write = is_write,
            .status = -1,
            .node_stride_bytes = client->node_stride_bytes,
            .interleave_bytes = client->interleave_bytes,
            .num_nodes = client->num_nodes,
        };
        if (pthread_create(&threads[lane], NULL, legomem_io_worker,
                           &ios[lane]) == 0) {
            started[lane] = 1;
        } else {
            legomem_io_worker(&ios[lane]);
        }
    }

    int status = 0;
    for (int lane = 0; lane < lanes; ++lane) {
        if (started[lane])
            pthread_join(threads[lane], NULL);
        if (ios[lane].status != 0)
            status = ios[lane].status;
    }
    return status;
}

void *legomem_client_open(const char *host, int port) {
    legomem_client_t *client = calloc(1, sizeof(*client));
    if (!client)
        return NULL;
    /* One lane is fastest on the 2-vCPU r7i.large test nodes.  Larger hosts
     * can opt into pipelining with LEGOMEM_IO_LANES. */
    client->num_lanes = 1;
    const char *lanes_env = getenv("LEGOMEM_IO_LANES");
    if (lanes_env) {
        long requested = strtol(lanes_env, NULL, 10);
        if (requested >= 1 && requested <= LEGOMEM_MAX_IO_LANES)
            client->num_lanes = (int)requested;
    }
    const char *stride_env = getenv("LEGOMEM_NODE_STRIDE_MIB");
    if (stride_env) {
        char *end = NULL;
        unsigned long long stride_mib = strtoull(stride_env, &end, 10);
        if (end && *end == '\0' && stride_mib > 0 &&
            stride_mib <= UINT64_MAX / LEGOMEM_MIB) {
            client->node_stride_bytes = (uint64_t)stride_mib * LEGOMEM_MIB;
        }
    }
    const char *nodes_env = getenv("LEGOMEM_NUM_NODES");
    if (nodes_env) {
        char *end = NULL;
        unsigned long nodes = strtoul(nodes_env, &end, 10);
        if (end && *end == '\0' && nodes >= 1 && nodes <= UINT32_MAX)
            client->num_nodes = (uint32_t)nodes;
    }
    const char *interleave_env = getenv("LEGOMEM_INTERLEAVE_MIB");
    if (interleave_env) {
        char *end = NULL;
        unsigned long long interleave_mib = strtoull(interleave_env, &end, 10);
        if (end && *end == '\0' && interleave_mib > 0 &&
            interleave_mib <= UINT64_MAX / LEGOMEM_MIB) {
            client->interleave_bytes = (uint64_t)interleave_mib * LEGOMEM_MIB;
        }
    }
    if (client->interleave_bytes > 0 &&
        (client->node_stride_bytes == 0 || client->num_nodes < 2)) {
        free(client);
        return NULL;
    }
    for (int lane = 0; lane < client->num_lanes; ++lane) {
        if (cxlmemsim_init(&client->ctx[lane], host, port) != 0 ||
            cxlmemsim_connect(&client->ctx[lane]) != 0) {
            for (int initialized = 0; initialized <= lane; ++initialized)
                cxlmemsim_finalize(&client->ctx[initialized]);
            free(client);
            return NULL;
        }
    }
    return client;
}

int legomem_client_read(void *opaque, uint64_t address, void *data,
                        size_t size) {
    if (!opaque || !data || size == 0)
        return -1;
    legomem_client_t *client = opaque;
    return legomem_transfer(client, address, data, size, 0);
}

int legomem_client_write(void *opaque, uint64_t address, const void *data,
                         size_t size) {
    if (!opaque || !data || size == 0)
        return -1;
    legomem_client_t *client = opaque;
    return legomem_transfer(client, address, (void *)data, size, 1);
}

void legomem_client_close(void *opaque) {
    if (!opaque)
        return;
    legomem_client_t *client = opaque;
    for (int lane = 0; lane < client->num_lanes; ++lane)
        cxlmemsim_finalize(&client->ctx[lane]);
    free(client);
}

uint64_t legomem_client_bytes_read(void *opaque) {
    if (!opaque)
        return 0;
    legomem_client_t *client = opaque;
    uint64_t total = 0;
    for (int lane = 0; lane < client->num_lanes; ++lane)
        total += client->ctx[lane].total_bytes_read;
    return total;
}

uint64_t legomem_client_bytes_written(void *opaque) {
    if (!opaque)
        return 0;
    legomem_client_t *client = opaque;
    uint64_t total = 0;
    for (int lane = 0; lane < client->num_lanes; ++lane)
        total += client->ctx[lane].total_bytes_written;
    return total;
}
