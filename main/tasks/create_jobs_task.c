#include <sys/time.h>
#include <limits.h>
#include <stdint.h>

#include "work_queue.h"
#include "global_state.h"
#include "esp_log.h"
#include "esp_system.h"
#include "mining.h"
#include "string.h"
#include "esp_timer.h"

#include "asic.h"
#include "system.h"
#include "esp_heap_caps.h"
#include "sv2_protocol.h"
#include "stratum_api.h"
#include "stratum_v2_task.h"
#include "utils.h"

static const char *TAG = "create_jobs_task";

#define MAX_EXTRANONCE2_LEN 32
#define MAX_EXTRANONCE2_STR (MAX_EXTRANONCE2_LEN * 2 + 1)

// Globale teller voor timestamp-rolling (alleen V1)
static uint32_t ntime_offset = 0;

static void generate_work(GlobalState *GLOBAL_STATE, mining_notify *notification,
                          uint64_t extranonce_2, double difficulty);
static void generate_work_sv2(GlobalState *GLOBAL_STATE, sv2_job_t *job, double difficulty);
static void generate_work_sv2_ext(GlobalState *GLOBAL_STATE, sv2_ext_job_t *job,
                                   double difficulty, uint64_t extranonce_2_counter);

// ... (free_work_item blijft ongewijzigd) ...

void create_jobs_task(void *pvParameters)
{
    GlobalState *GLOBAL_STATE = (GlobalState *)pvParameters;

    double difficulty = GLOBAL_STATE->pool_difficulty;
    void *current_work = NULL;
    stratum_protocol_t current_work_protocol = GLOBAL_STATE->stratum_protocol;
    uint64_t extranonce_2 = 0;

    int timeout_ms = 4000;  // vaste interval

    ESP_LOGI(TAG, "ASIC Job Interval: %d ms", timeout_ms);
    ESP_LOGI(TAG, "ASIC Ready!");

    while (1) {
        // Reset extranonce2 indien gevraagd (bv. na set_extranonce)
        if (GLOBAL_STATE->reset_extranonce2) {
            ESP_LOGI(TAG, "Resetting extranonce2 to 0 due to set_extranonce request");
            extranonce_2 = 0;
            GLOBAL_STATE->reset_extranonce2 = false;
            ntime_offset = 0;
        }

        stratum_protocol_t active_protocol = GLOBAL_STATE->stratum_protocol;

        // Protocolwissel afhandelen
        if (active_protocol != current_work_protocol) {
            if (current_work != NULL) {
                ESP_LOGI(TAG, "Protocol switched from %s to %s, discarding current work",
                         current_work_protocol == STRATUM_PROTOCOL_V2 ? "V2" : "V1",
                         active_protocol == STRATUM_PROTOCOL_V2 ? "V2" : "V1");
                free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
                current_work = NULL;
            }
            current_work_protocol = active_protocol;
        }

        // Wacht op nieuwe job (max timeout_ms)
        uint64_t start_time = esp_timer_get_time();
        void *new_work = queue_dequeue_timeout(&GLOBAL_STATE->stratum_queue, timeout_ms);
        timeout_ms -= (esp_timer_get_time() - start_time) / 1000;

        if (new_work != NULL) {
            // Nieuwe job ontvangen
            active_protocol = GLOBAL_STATE->stratum_protocol;

            // Oude work vrijgeven
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;

            if (active_protocol != current_work_protocol) {
                ESP_LOGW(TAG, "Protocol switch detected during dequeue, discarding stale item");
                free(new_work);
                current_work_protocol = active_protocol;
                timeout_ms = 4000;
                continue;
            }

            current_work = new_work;
            extranonce_2 = 0;   // reset voor nieuwe job
            ntime_offset = 0;   // ook timestamp resetten

            // Log ontvangen werk
            if (current_work_protocol == STRATUM_PROTOCOL_V2) {
                if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                    ESP_LOGI(TAG, "New Work Dequeued SV2 ext job %lu", ((sv2_ext_job_t *)current_work)->job_id);
                } else {
                    ESP_LOGI(TAG, "New Work Dequeued SV2 job %lu", ((sv2_job_t *)current_work)->job_id);
                }
            } else {
                ESP_LOGI(TAG, "New Work Dequeued %s", ((mining_notify *)current_work)->job_id);
            }

            // Nieuwe difficulty?
            if (GLOBAL_STATE->new_set_mining_difficulty_msg) {
                ESP_LOGI(TAG, "New pool difficulty %.2f", GLOBAL_STATE->pool_difficulty);
                difficulty = GLOBAL_STATE->pool_difficulty;
                GLOBAL_STATE->new_set_mining_difficulty_msg = false;
            }

            // Version rolling mask update
            if (GLOBAL_STATE->new_stratum_version_rolling_msg && GLOBAL_STATE->ASIC_initalized) {
                ESP_LOGI(TAG, "Set chip version rolls %i", (int)(GLOBAL_STATE->version_mask >> 13));
                ASIC_set_version_mask(GLOBAL_STATE, GLOBAL_STATE->version_mask);
                GLOBAL_STATE->new_stratum_version_rolling_msg = false;
            }

            // Check of het een clean job is (dan ntime_offset op 0 laten)
            bool clean;
            if (current_work_protocol == STRATUM_PROTOCOL_V2) {
                if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                    clean = ((sv2_ext_job_t *)current_work)->clean_jobs;
                } else {
                    clean = ((sv2_job_t *)current_work)->clean_jobs;
                }
            } else {
                clean = ((mining_notify *)current_work)->clean_jobs;
            }

            if (clean) {
                ntime_offset = 0;
                ESP_LOGI(TAG, "Clean job: timestamp offset reset to 0");
            }

            // Als de job niet clean is, dan overslaan? Nee, we willen hem wel gebruiken,
            // maar we moeten de offset niet resetten. De job is al geldig.
            // We gaan gewoon door.
        } else {
            // Geen nieuwe job, gebruik de huidige (als die bestaat)
            if (current_work == NULL) {
                vTaskDelay(100 / portTICK_PERIOD_MS);
                timeout_ms = 4000;
                continue;
            }
            // Voor SV2 standaard kanaal (geen extended) wordt geen werk gegenereerd zonder nieuwe job
            if (active_protocol == STRATUM_PROTOCOL_V2 && !stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                timeout_ms = 4000;
                continue;
            }
        }

        // Protocol opnieuw controleren (kan tussendoor veranderd zijn)
        active_protocol = GLOBAL_STATE->stratum_protocol;
        if (active_protocol != current_work_protocol) {
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;
            current_work_protocol = active_protocol;
            timeout_ms = 4000;
            continue;
        }

        // --- Genereer en stuur een taak naar de ASIC ---
        if (active_protocol == STRATUM_PROTOCOL_V2) {
            if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                generate_work_sv2_ext(GLOBAL_STATE, (sv2_ext_job_t *)current_work, difficulty, extranonce_2);
                extranonce_2++;   // kleine stap voor SV2 extended
            } else {
                generate_work_sv2(GLOBAL_STATE, (sv2_job_t *)current_work, difficulty);
                // SV2 standaard kanaal heeft geen extranonce2, dus geen verhoging
            }
        } else {
            // V1: gebruik extranonce_2 en ntime_offset
            ESP_LOGI(TAG, "Generating V1 job with extranonce2=%llu, ntime_offset=%u",
                     extranonce_2, ntime_offset);
            generate_work(GLOBAL_STATE, (mining_notify *)current_work, extranonce_2, difficulty);

            // --- VERHOGINGEN (cruciaal!) ---
            // Grotere stap om overlapping te voorkomen (0x10000 = 65536)
            extranonce_2 += 0x10000;
            if (extranonce_2 > UINT64_MAX - 0x10000) extranonce_2 = 0;

            // Timestamp rolling: verhoog met 1, max 1 uur
            ntime_offset++;
            if (ntime_offset > 3600) ntime_offset = 0;
        }

        // Reset timeout voor volgende iteratie
        timeout_ms = 4000;
    }
}

// ==================== generate_work (V1) ====================
static void generate_work(GlobalState *GLOBAL_STATE, mining_notify *notification,
                          uint64_t extranonce_2, double difficulty)
{
    if (GLOBAL_STATE->extranonce_2_len > MAX_EXTRANONCE2_LEN) {
        ESP_LOGE(TAG, "extranonce_2_len %d exceeds maximum %d, skipping job",
                 GLOBAL_STATE->extranonce_2_len, MAX_EXTRANONCE2_LEN);
        return;
    }
    char extranonce_2_str[MAX_EXTRANONCE2_STR];
    extranonce_2_generate(extranonce_2, GLOBAL_STATE->extranonce_2_len, extranonce_2_str);

    uint8_t coinbase_tx_hash[32];
    calculate_coinbase_tx_hash(notification->coinbase_1, notification->coinbase_2,
                               GLOBAL_STATE->extranonce_str, extranonce_2_str,
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

    construct_bm_job(notification, merkle_root, GLOBAL_STATE->version_mask,
                     difficulty, next_job);

    // Timestamp rolling (gebruik de globale ntime_offset)
    #define MAX_NTIME_OFFSET 3600
    next_job->ntime = notification->ntime + ntime_offset;

    // Startnonce gebaseerd op extranonce_2 (deterministisch)
    next_job->starting_nonce = (uint32_t)((extranonce_2 * 0x9e3779b9ULL) & 0xFFFFFFFF);

    // Metadata
    next_job->extranonce2 = strdup(extranonce_2_str);
    next_job->jobid = strdup(notification->job_id);
    next_job->version_mask = GLOBAL_STATE->version_mask;

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ASIC_send_work(GLOBAL_STATE, next_job);
}

// De SV2-functies blijven ongewijzigd (zijn niet betrokken bij de bug)
// ... (generate_work_sv2 en generate_work_sv2_ext)
