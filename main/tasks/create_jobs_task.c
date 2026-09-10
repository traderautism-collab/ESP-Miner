// ============================================================
// create_jobs_task.c
// Volledige implementatie: version rolling + job dispatch
// ============================================================

#include <sys/time.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "work_queue.h"
#include "global_state.h"
#include "esp_log.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "mining.h"
#include "asic.h"
#include "system.h"
#include "sv2_protocol.h"
#include "stratum_api.h"
#include "stratum_v2_task.h"
#include "utils.h"

static const char *TAG = "create_jobs_task";

#define MAX_EXTRANONCE2_LEN 32
#define MAX_EXTRANONCE2_STR (MAX_EXTRANONCE2_LEN * 2 + 1)

// ============================================================
// 1. VERSION ROLLING HELPERS
// ============================================================

/**
 * Tel 1 op bij de laagste vrije bit binnen de mask.
 * Alleen bits die 1 zijn in de mask mogen veranderen.
 */
static uint32_t increment_bitmask(uint32_t version, uint32_t mask)
{
    uint32_t new_version = version + 1;
    return (version & ~mask) | (new_version & mask);
}

/**
 * Valideer de mask volgens BIP320.
 * Alleen bits 13..28 mogen rollen.
 */
static bool is_valid_version_mask(uint32_t mask)
{
    const uint32_t BIP320_ALLOWED = 0x1FFFE000;

    if (mask == 0) return false;
    if (mask & ~BIP320_ALLOWED) return false;

    // Minimaal 2 roll-bits zoals BIP320 voorschrijft
    if (__builtin_popcount(mask) < 2) return false;

    return true;
}

/**
 * Verwerk een nieuwe version mask van de pool.
 * Wordt aangeroepen door stratum_api.c of sv2_protocol.c
 */
void stratum_handle_set_version_mask(GlobalState *GLOBAL_STATE, uint32_t new_mask)
{
    if (!is_valid_version_mask(new_mask)) {
        ESP_LOGW(TAG, "Ongeldige version mask 0x%08" PRIx32 " van pool, genegeerd",
                 new_mask);
        return;
    }

    if (new_mask == GLOBAL_STATE->version_mask) {
        ESP_LOGD(TAG, "Version mask ongewijzigd: 0x%08" PRIx32, new_mask);
        return;
    }

    ESP_LOGI(TAG, "Nieuwe version mask: 0x%08" PRIx32 " (%d roll-bits)",
             new_mask, __builtin_popcount(new_mask));

    GLOBAL_STATE->version_mask = new_mask;
    GLOBAL_STATE->new_stratum_version_rolling_msg = true;
    GLOBAL_STATE->version_rolling_negotiated = true;
}

/**
 * Pas de mask toe op de ASIC (alleen als hij veranderd is).
 */
static void apply_version_mask_to_asic(GlobalState *GLOBAL_STATE)
{
    if (!GLOBAL_STATE->new_stratum_version_rolling_msg) return;
    if (!GLOBAL_STATE->ASIC_initalized) return;

    uint32_t mask = GLOBAL_STATE->version_mask;

    ESP_LOGI(TAG, "ASIC version mask update: 0x%08" PRIx32 " (%d roll-bits)",
             mask, __builtin_popcount(mask));

    ASIC_set_version_mask(GLOBAL_STATE, mask);

    GLOBAL_STATE->new_stratum_version_rolling_msg = false;
}

// ============================================================
// 2. FREE HELPERS (per protocol)
// ============================================================

static void free_work_item(GlobalState *GLOBAL_STATE, void *work, stratum_protocol_t protocol)
{
    if (!work) return;

    if (protocol == STRATUM_PROTOCOL_V2) {
        if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
            sv2_ext_job_free((sv2_ext_job_t *)work);
        } else {
            free(work);
        }
    } else {
        STRATUM_V1_free_mining_notify(work);
    }
}

// ============================================================
// 3. JOB CONSTRUCTIE — STRATUM V1
// ============================================================

static void generate_work(GlobalState *GLOBAL_STATE, mining_notify *notification, double difficulty)
{
    if (GLOBAL_STATE->extranonce_2_len > MAX_EXTRANONCE2_LEN) {
        ESP_LOGE(TAG, "extranonce_2_len %d exceeds maximum %d",
                 GLOBAL_STATE->extranonce_2_len, MAX_EXTRANONCE2_LEN);
        return;
    }

    // Extranonce2 = 0 (industrie standaard)
    char extranonce_2_str[MAX_EXTRANONCE2_STR];
    memset(extranonce_2_str, '0', GLOBAL_STATE->extranonce_2_len * 2);
    extranonce_2_str[GLOBAL_STATE->extranonce_2_len * 2] = '\0';

    // Coinbase + merkle root
    uint8_t coinbase_tx_hash[32];
    calculate_coinbase_tx_hash(notification->coinbase_1,
                               notification->coinbase_2,
                               GLOBAL_STATE->extranonce_str,
                               extranonce_2_str,
                               coinbase_tx_hash);

    uint8_t merkle_root[32];
    calculate_merkle_root_hash(coinbase_tx_hash,
                               (uint8_t(*)[32])notification->merkle_branches,
                               notification->n_merkle_branches,
                               merkle_root);

    bm_job *next_job = malloc(sizeof(bm_job));
    if (next_job == NULL) {
        ESP_LOGE(TAG, "Failed to allocate memory for new job");
        return;
    }
    memset(next_job, 0, sizeof(bm_job));

    // Bouw job met version rolling
    construct_bm_job(notification, merkle_root, GLOBAL_STATE->version_mask,
                     difficulty, next_job);

    next_job->extranonce2  = strdup(extranonce_2_str);
    next_job->jobid        = strdup(notification->job_id);
    next_job->version_mask = GLOBAL_STATE->version_mask;

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ESP_LOGI(TAG, "V1 job %s -> ASIC (mask=0x%08" PRIx32 ", midstates=%d)",
             notification->job_id, GLOBAL_STATE->version_mask,
             next_job->num_midstates);

    ASIC_send_work(GLOBAL_STATE, next_job);
}

// ============================================================
// 4. JOB CONSTRUCTIE — STRATUM V2 (regular)
// ============================================================

static void generate_work_sv2(GlobalState *GLOBAL_STATE, sv2_job_t *sv2_job, double difficulty)
{
    bm_job *next_job = malloc(sizeof(bm_job));
    if (next_job == NULL) {
        ESP_LOGE(TAG, "Failed to allocate memory for new SV2 job");
        return;
    }
    memset(next_job, 0, sizeof(bm_job));

    uint32_t version_mask = GLOBAL_STATE->version_mask;

    next_job->version        = sv2_job->version;
    next_job->target         = sv2_job->nbits;
    next_job->ntime          = sv2_job->ntime;
    next_job->starting_nonce = 0;
    next_job->pool_diff      = difficulty;
    next_job->version_mask   = version_mask;

    reverse_32bit_words(sv2_job->merkle_root, next_job->merkle_root);
    reverse_32bit_words(sv2_job->prev_hash, next_job->prev_block_hash);

    uint8_t midstate_data[64];
    uint32_t base_version = sv2_job->version;
    memcpy(midstate_data, &base_version, 4);
    memcpy(midstate_data + 4, sv2_job->prev_hash, 32);
    memcpy(midstate_data + 36, sv2_job->merkle_root, 28);

    uint8_t midstate[32];
    midstate_sha256_bin(midstate_data, 64, midstate);
    reverse_32bit_words(midstate, next_job->midstate);

    if (version_mask != 0) {
        uint32_t rolled = increment_bitmask(base_version, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate1);

        rolled = increment_bitmask(rolled, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate2);

        rolled = increment_bitmask(rolled, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate3);

        next_job->num_midstates = 4;
    } else {
        next_job->num_midstates = 1;
    }

    char jobid_str[16];
    snprintf(jobid_str, sizeof(jobid_str), "%" PRIu32, sv2_job->job_id);
    next_job->jobid       = strdup(jobid_str);
    next_job->extranonce2 = strdup("");

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping SV2 job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ESP_LOGI(TAG, "SV2 job %" PRIu32 " -> ASIC (mask=0x%08" PRIx32 ", midstates=%d)",
             sv2_job->job_id, version_mask, next_job->num_midstates);

    ASIC_send_work(GLOBAL_STATE, next_job);
}

// ============================================================
// 5. JOB CONSTRUCTIE — STRATUM V2 (extended)
// ============================================================

static void generate_work_sv2_ext(GlobalState *GLOBAL_STATE, sv2_ext_job_t *ext_job, double difficulty)
{
    sv2_conn_t *conn = GLOBAL_STATE->sv2_conn;
    if (!conn) return;

    bm_job *next_job = malloc(sizeof(bm_job));
    if (!next_job) {
        ESP_LOGE(TAG, "Failed to allocate memory for SV2 ext job");
        return;
    }
    memset(next_job, 0, sizeof(bm_job));

    uint32_t version_mask = GLOBAL_STATE->version_mask;

    uint8_t extranonce_2_len = conn->extranonce_size;
    uint8_t extranonce_2[32];
    memset(extranonce_2, 0, sizeof(extranonce_2));

    uint8_t coinbase_tx_hash[32];
    calculate_coinbase_tx_hash_bin(
        ext_job->coinbase_prefix, ext_job->coinbase_prefix_len,
        conn->extranonce_prefix, conn->extranonce_prefix_len,
        extranonce_2, extranonce_2_len,
        ext_job->coinbase_suffix, ext_job->coinbase_suffix_len,
        coinbase_tx_hash);

    uint8_t merkle_root[32];
    calculate_merkle_root_hash(coinbase_tx_hash,
                               (const uint8_t (*)[32])ext_job->merkle_path,
                               ext_job->merkle_path_count,
                               merkle_root);

    next_job->version        = ext_job->version;
    next_job->target         = ext_job->nbits;
    next_job->ntime          = ext_job->ntime;
    next_job->starting_nonce = 0;
    next_job->pool_diff      = difficulty;
    next_job->version_mask   = version_mask;

    reverse_32bit_words(merkle_root, next_job->merkle_root);
    reverse_32bit_words(ext_job->prev_hash, next_job->prev_block_hash);

    uint8_t midstate_data[64];
    uint32_t base_version = ext_job->version;
    memcpy(midstate_data, &base_version, 4);
    memcpy(midstate_data + 4, ext_job->prev_hash, 32);
    memcpy(midstate_data + 36, merkle_root, 28);

    uint8_t midstate[32];
    midstate_sha256_bin(midstate_data, 64, midstate);
    reverse_32bit_words(midstate, next_job->midstate);

    if (version_mask != 0) {
        uint32_t rolled = increment_bitmask(base_version, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate1);

        rolled = increment_bitmask(rolled, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate2);

        rolled = increment_bitmask(rolled, version_mask);
        memcpy(midstate_data, &rolled, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate3);

        next_job->num_midstates = 4;
    } else {
        next_job->num_midstates = 1;
    }

    char jobid_str[16];
    snprintf(jobid_str, sizeof(jobid_str), "%" PRIu32, ext_job->job_id);
    next_job->jobid = strdup(jobid_str);

    char en2_hex[65];
    bin2hex(extranonce_2, extranonce_2_len, en2_hex, sizeof(en2_hex));
    next_job->extranonce2 = strdup(en2_hex);

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping SV2 ext job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ESP_LOGI(TAG, "SV2 ext job %" PRIu32 " -> ASIC (mask=0x%08" PRIx32 ", midstates=%d)",
             ext_job->job_id, version_mask, next_job->num_midstates);

    ASIC_send_work(GLOBAL_STATE, next_job);
}

// ============================================================
// 6. HOOFD TAAK — create_jobs_task
// ============================================================

void create_jobs_task(void *pvParameters)
{
    GlobalState *GLOBAL_STATE = (GlobalState *)pvParameters;

    double difficulty = GLOBAL_STATE->pool_difficulty;
    void *current_work = NULL;
    stratum_protocol_t current_work_protocol = GLOBAL_STATE->stratum_protocol;
    int timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);

    // Onthoud laatst verzonden job-ID's
    static char last_dispatched_job_v1[64] = {0};
    static uint32_t last_dispatched_job_sv2 = UINT32_MAX;

    ESP_LOGI(TAG, "ASIC Job Interval: %d ms", timeout_ms);
    ESP_LOGI(TAG, "ASIC Ready! (Version Rolling + BIP320 + Auto-Job-Update)");
    ESP_LOGI(TAG, "Version rolling negotiated: %s",
             GLOBAL_STATE->version_rolling_negotiated ? "yes" : "no");

    while (1) {
        // === 1. Version mask update naar ASIC ===
        apply_version_mask_to_asic(GLOBAL_STATE);

        // === 2. Reset extranonce2 flag ===
        if (GLOBAL_STATE->reset_extranonce2) {
            GLOBAL_STATE->reset_extranonce2 = false;
        }

        // === 3. Protocol wissel detectie ===
        stratum_protocol_t active_protocol = GLOBAL_STATE->stratum_protocol;
        if (active_protocol != current_work_protocol) {
            if (current_work != NULL) {
                ESP_LOGI(TAG, "Protocol switch %s -> %s, discarding work",
                         current_work_protocol == STRATUM_PROTOCOL_V2 ? "V2" : "V1",
                         active_protocol == STRATUM_PROTOCOL_V2 ? "V2" : "V1");
                free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
                current_work = NULL;
            }
            current_work_protocol = active_protocol;
            last_dispatched_job_v1[0] = '\0';
            last_dispatched_job_sv2 = UINT32_MAX;
        }

        // === 4. Wacht op nieuwe job ===
        uint64_t start_time = esp_timer_get_time();
        void *new_work = queue_dequeue_timeout(&GLOBAL_STATE->stratum_queue, timeout_ms);
        timeout_ms -= (esp_timer_get_time() - start_time) / 1000;

        if (new_work != NULL) {
            active_protocol = GLOBAL_STATE->stratum_protocol;

            // Oude job vrijgeven
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;

            // Protocol wissel tijdens dequeue?
            if (active_protocol != current_work_protocol) {
                ESP_LOGW(TAG, "Protocol switch detected during dequeue, discarding");
                free(new_work);
                current_work_protocol = active_protocol;
                timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
                continue;
            }

            current_work = new_work;

            // === 5. Job info + deduplicatie ===
            bool is_new_job_id = false;
            bool clean = false;

            if (current_work_protocol == STRATUM_PROTOCOL_V2) {
                if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                    sv2_ext_job_t *j = (sv2_ext_job_t *)current_work;
                    ESP_LOGI(TAG, "New Work Dequeued SV2 ext job %" PRIu32, j->job_id);
                    clean = j->clean_jobs;
                    if (last_dispatched_job_sv2 != j->job_id) {
                        is_new_job_id = true;
                        last_dispatched_job_sv2 = j->job_id;
                    }
                } else {
                    sv2_job_t *j = (sv2_job_t *)current_work;
                    ESP_LOGI(TAG, "New Work Dequeued SV2 job %" PRIu32, j->job_id);
                    clean = j->clean_jobs;
                    if (last_dispatched_job_sv2 != j->job_id) {
                        is_new_job_id = true;
                        last_dispatched_job_sv2 = j->job_id;
                    }
                }
            } else {
                mining_notify *j = (mining_notify *)current_work;
                ESP_LOGI(TAG, "New Work Dequeued %s (clean: %s)",
                         j->job_id, j->clean_jobs ? "true" : "false");
                clean = j->clean_jobs;
                if (strcmp(last_dispatched_job_v1, j->job_id) != 0) {
                    is_new_job_id = true;
                    strncpy(last_dispatched_job_v1, j->job_id,
                            sizeof(last_dispatched_job_v1) - 1);
                }
            }

            // === 6. Difficulty update ===
            if (GLOBAL_STATE->new_set_mining_difficulty_msg) {
                ESP_LOGI(TAG, "New pool difficulty %.2f", GLOBAL_STATE->pool_difficulty);
                difficulty = GLOBAL_STATE->pool_difficulty;
                GLOBAL_STATE->new_set_mining_difficulty_msg = false;
            }

            // === 7. Version rolling update (extra check) ===
            if (GLOBAL_STATE->new_stratum_version_rolling_msg &&
                GLOBAL_STATE->ASIC_initalized) {
                apply_version_mask_to_asic(GLOBAL_STATE);
            }

            // === 8. Skip zelfde job zonder clean ===
            if (!is_new_job_id && !clean) {
                continue;
            }

        } else {
            // Queue leeg (timeout)
            if (current_work == NULL) {
                vTaskDelay(100 / portTICK_PERIOD_MS);
                continue;
            }
            timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
            continue;
        }

        // === 9. Laatste protocol check ===
        active_protocol = GLOBAL_STATE->stratum_protocol;
        if (active_protocol != current_work_protocol) {
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;
            current_work_protocol = active_protocol;
            timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
            continue;
        }

        // === 10. Job naar ASIC ===
        if (active_protocol == STRATUM_PROTOCOL_V2) {
            if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                generate_work_sv2_ext(GLOBAL_STATE,
                                      (sv2_ext_job_t *)current_work,
                                      difficulty);
            } else {
                generate_work_sv2(GLOBAL_STATE,
                                  (sv2_job_t *)current_work,
                                  difficulty);
            }
        } else {
            generate_work(GLOBAL_STATE,
                          (mining_notify *)current_work,
                          difficulty);
        }

        timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
    }
}
