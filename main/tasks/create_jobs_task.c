#include <sys/time.h>
#include <limits.h>

#include "work_queue.h"
#include "global_state.h"
#include "esp_log.h"
#include "esp_system.h"
#include "esp_random.h"
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

// Golden Ratio constants
#define STEP_GOLDEN_RATIO_64 0x9E3779B97F4A7C15ULL
#define STEP_GOLDEN_RATIO_32 0x9E3779B9ULL

// Optimale STAP voor 1.2 TH/s - gebruik een KLEINE stap!
// Dit zorgt dat je veel opeenvolgende waarden test
#define STEP_OPTIMAL_SMALL_1 0x00010001ULL  // 65.537
#define STEP_OPTIMAL_SMALL_2 0x00020003ULL  // 131.075
#define STEP_OPTIMAL_SMALL_3 0x00040007ULL  // 262.151
#define STEP_OPTIMAL_SMALL_4 0x0008000BULL  // 524.299

// Hoeveelheid unieke waarden per job (bij 1.2 TH/s, 10 minuten)
// 7.2e14 hashes / 4.29e9 = ~167.000 keer de hele ruimte
// Dus we kunnen VEEL waarden testen!
#define WAARDEN_PER_JOB 1000000  // 1 miljoen unieke waarden per job

static void generate_work(GlobalState *GLOBAL_STATE, mining_notify *notification, uint64_t extranonce_2, double difficulty);
static void generate_work_sv2(GlobalState *GLOBAL_STATE, sv2_job_t *job, double difficulty);
static void generate_work_sv2_ext(GlobalState *GLOBAL_STATE, sv2_ext_job_t *job, double difficulty, uint64_t extranonce_2_counter);

// Bepaal de juiste stapgrootte - gebruik KLEINE stappen!
static inline uint64_t get_extranonce2_step(uint8_t len)
{
    if (len >= 8) {
        return STEP_GOLDEN_RATIO_64;
    }
    
    // Voor 4 bytes: gebruik een kleine, oneven stap
    // Dit zorgt dat we veel opeenvolgende waarden testen
    uint32_t random = esp_random();
    uint64_t step;
    
    switch (random % 4) {
        case 0: step = STEP_OPTIMAL_SMALL_1; break;
        case 1: step = STEP_OPTIMAL_SMALL_2; break;
        case 2: step = STEP_OPTIMAL_SMALL_3; break;
        default: step = STEP_OPTIMAL_SMALL_4; break;
    }
    
    // Zorg dat het oneven is (relatief priem t.o.v. macht van 2)
    if ((step & 1) == 0) {
        step |= 1;
    }
    
    return step;
}

// Pas maskering toe
static inline uint64_t mask_extranonce2(uint64_t val, uint8_t len)
{
    if (len >= 8) {
        return val;
    }
    uint64_t mask = (1ULL << (len * 8)) - 1ULL;
    return val & mask;
}

// Genereer een willekeurige startwaarde
static inline uint64_t get_random_extranonce2_start(uint8_t len)
{
    uint64_t start;
    if (len >= 8) {
        start = ((uint64_t)esp_random() << 32) | esp_random();
    } else {
        start = esp_random();
    }
    return mask_extranonce2(start, len);
}

// Free a work item
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

void create_jobs_task(void *pvParameters)
{
    GlobalState *GLOBAL_STATE = (GlobalState *)pvParameters;

    double difficulty = GLOBAL_STATE->pool_difficulty;
    void *current_work = NULL;
    stratum_protocol_t current_work_protocol = GLOBAL_STATE->stratum_protocol;
    
    // 🔥 START MET EEN WILLEKEURIGE WAARDE
    uint64_t extranonce_2 = get_random_extranonce2_start(GLOBAL_STATE->extranonce_2_len);
    uint64_t extranonce_2_step = get_extranonce2_step(GLOBAL_STATE->extranonce_2_len);
    extranonce_2_step = mask_extranonce2(extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
    if ((extranonce_2_step & 1) == 0) {
        extranonce_2_step |= 1;
    }

    // 🔥 BEREKEN HOEVEEL WAARDEN WE KUNNEN TESTEN
    uint64_t waarden_per_job = WAARDEN_PER_JOB;
    if (GLOBAL_STATE->extranonce_2_len < 8) {
        // Voor 4 bytes: max 4.29 miljard, we testen 1 miljoen per job
        // Dat is ~0.023% van de ruimte per job
        // Na 100 jobs hebben we ~2.3% gedekt
        // Na 1000 jobs ~23%
        // Na 4300 jobs de hele ruimte
        ESP_LOGI(TAG, "📊 We testen ~%llu unieke waarden per job (van de %llu mogelijk)", 
                 (unsigned long long)waarden_per_job,
                 (unsigned long long)(1ULL << (GLOBAL_STATE->extranonce_2_len * 8)));
    }

    ESP_LOGI(TAG, "🚀 Golden Ratio optimalisatie met KLEINE stap voor 1.2 TH/s");
    ESP_LOGI(TAG, "   Start extranonce2: 0x%llx", (unsigned long long)extranonce_2);
    ESP_LOGI(TAG, "   Stapgrootte: 0x%llx (%llu)", 
             (unsigned long long)extranonce_2_step, 
             (unsigned long long)extranonce_2_step);
    ESP_LOGI(TAG, "   Extranonce2 lengte: %d bytes", GLOBAL_STATE->extranonce_2_len);

    int timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
    ESP_LOGI(TAG, "ASIC Job Interval: %d ms", timeout_ms);
    ESP_LOGI(TAG, "ASIC Ready!");

    uint64_t jobs_verwerkt = 0;
    uint64_t total_hashes_estimate = 0;
    uint64_t start_time_total = esp_timer_get_time();
    uint64_t start_job_counter = extranonce_2;  // Voor tracking

    while (1) {
        // Check voor reset
        if (GLOBAL_STATE->reset_extranonce2) {
            extranonce_2 = get_random_extranonce2_start(GLOBAL_STATE->extranonce_2_len);
            extranonce_2_step = get_extranonce2_step(GLOBAL_STATE->extranonce_2_len);
            extranonce_2_step = mask_extranonce2(extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
            if ((extranonce_2_step & 1) == 0) {
                extranonce_2_step |= 1;
            }
            
            ESP_LOGI(TAG, "🔄 Reset - Start: 0x%llx, Stap: 0x%llx", 
                     (unsigned long long)extranonce_2, 
                     (unsigned long long)extranonce_2_step);
            GLOBAL_STATE->reset_extranonce2 = false;
            jobs_verwerkt = 0;
            start_job_counter = extranonce_2;
        }

        // Read protocol
        stratum_protocol_t active_protocol = GLOBAL_STATE->stratum_protocol;

        if (active_protocol != current_work_protocol) {
            if (current_work != NULL) {
                ESP_LOGI(TAG, "Protocol switched from %s to %s",
                         current_work_protocol == STRATUM_PROTOCOL_V2 ? STRATUM_V2 : STRATUM_V1,
                         active_protocol == STRATUM_PROTOCOL_V2 ? STRATUM_V2 : STRATUM_V1);
                free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
                current_work = NULL;
            }
            current_work_protocol = active_protocol;
        }

        uint64_t start_time = esp_timer_get_time();
        void *new_work = queue_dequeue_timeout(&GLOBAL_STATE->stratum_queue, timeout_ms);
        timeout_ms -= (esp_timer_get_time() - start_time) / 1000;

        if (new_work != NULL) {
            active_protocol = GLOBAL_STATE->stratum_protocol;
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;

            if (active_protocol != current_work_protocol) {
                ESP_LOGW(TAG, "Protocol switch detected during dequeue");
                free(new_work);
                current_work_protocol = active_protocol;
                timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
                continue;
            }

            if (current_work_protocol == STRATUM_PROTOCOL_V2) {
                if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                    ESP_LOGI(TAG, "New SV2 ext job %lu", ((sv2_ext_job_t *)new_work)->job_id);
                } else {
                    ESP_LOGI(TAG, "New SV2 job %lu", ((sv2_job_t *)new_work)->job_id);
                }
            } else {
                ESP_LOGI(TAG, "New job %s", ((mining_notify *)new_work)->job_id);
            }

            current_work = new_work;

            if (GLOBAL_STATE->new_set_mining_difficulty_msg) {
                ESP_LOGI(TAG, "New difficulty %.2f", GLOBAL_STATE->pool_difficulty);
                difficulty = GLOBAL_STATE->pool_difficulty;
                GLOBAL_STATE->new_set_mining_difficulty_msg = false;
            }

            if (GLOBAL_STATE->new_stratum_version_rolling_msg && GLOBAL_STATE->ASIC_initalized) {
                ESP_LOGI(TAG, "Set version rolls %i", (int)(GLOBAL_STATE->version_mask >> 13));
                ASIC_set_version_mask(GLOBAL_STATE, GLOBAL_STATE->version_mask);
                GLOBAL_STATE->new_stratum_version_rolling_msg = false;
            }

            // Check clean_jobs flag
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
            
            // 🔥 Bij CLEAN job: nieuwe random start voor maximale spreiding
            if (clean) {
                extranonce_2 = get_random_extranonce2_start(GLOBAL_STATE->extranonce_2_len);
                // Varieer de stap ook
                extranonce_2_step = get_extranonce2_step(GLOBAL_STATE->extranonce_2_len);
                extranonce_2_step = mask_extranonce2(extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
                if ((extranonce_2_step & 1) == 0) {
                    extranonce_2_step |= 1;
                }
                jobs_verwerkt = 0;
                start_job_counter = extranonce_2;
                
                ESP_LOGI(TAG, "✨ CLEAN job - Nieuwe start: 0x%llx, Stap: 0x%llx", 
                         (unsigned long long)extranonce_2, 
                         (unsigned long long)extranonce_2_step);
            }
        } else {
            if (current_work == NULL) {
                vTaskDelay(100 / portTICK_PERIOD_MS);
                continue;
            }
            if (active_protocol == STRATUM_PROTOCOL_V2 && !stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
                continue;
            }
        }

        active_protocol = GLOBAL_STATE->stratum_protocol;
        if (active_protocol != current_work_protocol) {
            free_work_item(GLOBAL_STATE, current_work, current_work_protocol);
            current_work = NULL;
            current_work_protocol = active_protocol;
            timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
            continue;
        }

        // 🔥 Generate en send job - met KLEINE stap
        if (jobs_verwerkt % 10000 == 0) {
            ESP_LOGI(TAG, "📊 Job #%llu - Extranonce2: 0x%llx (dec: %llu)", 
                     (unsigned long long)jobs_verwerkt,
                     (unsigned long long)extranonce_2, 
                     (unsigned long long)extranonce_2);
        }

        if (active_protocol == STRATUM_PROTOCOL_V2) {
            if (stratum_v2_is_extended_channel(GLOBAL_STATE)) {
                generate_work_sv2_ext(GLOBAL_STATE, (sv2_ext_job_t *)current_work, difficulty, extranonce_2);
                extranonce_2 = mask_extranonce2(extranonce_2 + extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
            } else {
                generate_work_sv2(GLOBAL_STATE, (sv2_job_t *)current_work, difficulty);
            }
        } else {
            generate_work(GLOBAL_STATE, (mining_notify *)current_work, extranonce_2, difficulty);
            extranonce_2 = mask_extranonce2(extranonce_2 + extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
        }
        
        jobs_verwerkt++;
        
        // 🔥 NA 1 MILJOEN WAARDEN: spring naar een NIEUWE random start
        // Dit voorkomt dat we in een klein gebied blijven hangen
        if (jobs_verwerkt >= WAARDEN_PER_JOB) {
            uint64_t nieuwe_start = get_random_extranonce2_start(GLOBAL_STATE->extranonce_2_len);
            
            // Zorg dat we niet op dezelfde plek blijven
            while (nieuwe_start == extranonce_2) {
                nieuwe_start = get_random_extranonce2_start(GLOBAL_STATE->extranonce_2_len);
            }
            
            extranonce_2 = nieuwe_start;
            start_job_counter = extranonce_2;
            
            // Varieer ook de stap
            extranonce_2_step = get_extranonce2_step(GLOBAL_STATE->extranonce_2_len);
            extranonce_2_step = mask_extranonce2(extranonce_2_step, GLOBAL_STATE->extranonce_2_len);
            if ((extranonce_2_step & 1) == 0) {
                extranonce_2_step |= 1;
            }
            
            ESP_LOGI(TAG, "🎯 Na %llu jobs (%.2f%% van ruimte), sprong naar nieuwe start: 0x%llx, stap: 0x%llx", 
                     (unsigned long long)jobs_verwerkt,
                     (float)(jobs_verwerkt * 100.0) / (1ULL << (GLOBAL_STATE->extranonce_2_len * 8)),
                     (unsigned long long)extranonce_2,
                     (unsigned long long)extranonce_2_step);
            
            jobs_verwerkt = 0;
        }
        
        // Toon statistieken elke 100.000 jobs
        if (jobs_verwerkt > 0 && jobs_verwerkt % 100000 == 0) {
            uint64_t current_time = esp_timer_get_time();
            uint64_t elapsed_sec = (current_time - start_time_total) / 1000000;
            if (elapsed_sec > 0) {
                uint64_t hashrate = (total_hashes_estimate * 1000) / elapsed_sec;
                float percentage = (float)(jobs_verwerkt * 100.0) / (1ULL << (GLOBAL_STATE->extranonce_2_len * 8));
                ESP_LOGI(TAG, "📈 Stats: %llu jobs, %.4f%% van ruimte, ~%llu H/s", 
                         (unsigned long long)jobs_verwerkt,
                         percentage,
                         (unsigned long long)hashrate);
            }
        }
        
        // Update hash estimate
        total_hashes_estimate += 1200000000000ULL; // 1.2 TH/s
        
        timeout_ms = ASIC_get_asic_job_frequency_ms(GLOBAL_STATE);
    }
}

static void generate_work(GlobalState *GLOBAL_STATE, mining_notify *notification, uint64_t extranonce_2, double difficulty)
{
    if (GLOBAL_STATE->extranonce_2_len > MAX_EXTRANONCE2_LEN) {
        ESP_LOGE(TAG, "extranonce_2_len %d exceeds maximum %d", GLOBAL_STATE->extranonce_2_len, MAX_EXTRANONCE2_LEN);
        return;
    }
    char extranonce_2_str[MAX_EXTRANONCE2_STR];
    extranonce_2_generate(extranonce_2, GLOBAL_STATE->extranonce_2_len, extranonce_2_str);

    uint8_t coinbase_tx_hash[32];
    calculate_coinbase_tx_hash(notification->coinbase_1, notification->coinbase_2, GLOBAL_STATE->extranonce_str, extranonce_2_str, coinbase_tx_hash);

    uint8_t merkle_root[32];
    calculate_merkle_root_hash(coinbase_tx_hash, (uint8_t(*)[32])notification->merkle_branches, notification->n_merkle_branches, merkle_root);

    bm_job *next_job = malloc(sizeof(bm_job));

    if (next_job == NULL) {
        ESP_LOGE(TAG, "Failed to allocate memory for new job");
        return;
    }

    construct_bm_job(notification, merkle_root, GLOBAL_STATE->version_mask, difficulty, next_job);

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

static void generate_work_sv2(GlobalState *GLOBAL_STATE, sv2_job_t *sv2_job, double difficulty)
{
    bm_job *next_job = malloc(sizeof(bm_job));
    if (next_job == NULL) {
        ESP_LOGE(TAG, "Failed to allocate memory for new SV2 job");
        return;
    }

    uint32_t version_mask = GLOBAL_STATE->version_mask;

    next_job->version = sv2_job->version;
    next_job->target = sv2_job->nbits;
    next_job->ntime = sv2_job->ntime;
    next_job->starting_nonce = 0;
    next_job->pool_diff = difficulty;

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
        uint32_t rolled_version = increment_bitmask(base_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate1);

        rolled_version = increment_bitmask(rolled_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate2);

        rolled_version = increment_bitmask(rolled_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate3);
        next_job->num_midstates = 4;
    } else {
        next_job->num_midstates = 1;
    }

    char jobid_str[16];
    snprintf(jobid_str, sizeof(jobid_str), "%" PRIu32, sv2_job->job_id);
    next_job->jobid = strdup(jobid_str);
    next_job->extranonce2 = strdup(""); 
    next_job->version_mask = version_mask;

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping SV2 job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ASIC_send_work(GLOBAL_STATE, next_job);
}

static void generate_work_sv2_ext(GlobalState *GLOBAL_STATE, sv2_ext_job_t *ext_job,
                                   double difficulty, uint64_t extranonce_2_counter)
{
    sv2_conn_t *conn = GLOBAL_STATE->sv2_conn;
    if (!conn) return;

    bm_job *next_job = malloc(sizeof(bm_job));
    if (!next_job) {
        ESP_LOGE(TAG, "Failed to allocate memory for SV2 ext job");
        return;
    }

    uint32_t version_mask = GLOBAL_STATE->version_mask;

    uint8_t extranonce_2_len = conn->extranonce_size;
    uint8_t extranonce_2[32];
    memset(extranonce_2, 0, sizeof(extranonce_2));
    
    uint64_t temp_counter = extranonce_2_counter;
    for (int i = extranonce_2_len - 1; i >= 0 && temp_counter > 0; i--) {
        extranonce_2[i] = (uint8_t)(temp_counter & 0xFF);
        temp_counter >>= 8;
    }

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
                               ext_job->merkle_path_count, merkle_root);

    next_job->version = ext_job->version;
    next_job->target = ext_job->nbits;
    next_job->ntime = ext_job->ntime;  
    next_job->starting_nonce = 0;
    next_job->pool_diff = difficulty;

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
        uint32_t rolled_version = increment_bitmask(base_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate1);

        rolled_version = increment_bitmask(rolled_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
        midstate_sha256_bin(midstate_data, 64, midstate);
        reverse_32bit_words(midstate, next_job->midstate2);

        rolled_version = increment_bitmask(rolled_version, version_mask);
        memcpy(midstate_data, &rolled_version, 4);
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
    next_job->version_mask = version_mask;

    if (!GLOBAL_STATE->ASIC_initalized) {
        ESP_LOGW(TAG, "ASIC not initialized, skipping SV2 ext job send");
        free(next_job->jobid);
        free(next_job->extranonce2);
        free(next_job);
        return;
    }

    ASIC_send_work(GLOBAL_STATE, next_job);
}
