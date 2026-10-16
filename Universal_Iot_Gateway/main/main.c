// AUTHOR : HARSHIT SINGH
// DATE : 27/08/2026

// HEADER FILES
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_log.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "esp_wifi.h"
#include "nvs_flash.h"
#include "esp_vfs_fat.h"
#include "driver/uart.h"
#include "esp_http_client.h"
#include "esp_wifi.h"
#include "esp_http_server.h"
#include "mqtt_client.h"
#include "driver/uart.h"
#include "cJSON.h" // This library file is used parsing the JASON 
#include <math.h>
#include "esp_crt_bundle.h"

// MACROS
// WIFI
#define WIFI_RETRY_THRESHOLD    (10)
// MQTT 
#define MQTT_BROKER_URI         "mqtts://mqtt.dashboard.hydroscope.in:8883"
#define MQTT_USER               "hydrosure"
#define MQTT_PASSWORD           "prod_mqtthydrosure2026"

// TOPIC
#define SUBSCRIBE_TOPIC         "UIgateway/config/Device/Modbus"
#define PUBLISH_TOPIC           "UIgateway/Device/Modbus/Data"
#define DEVICE_STATUS_TOPIC     "UIgateway/Device/Status"

// MODBUS 
#define MODBUS_RX_PIN           (18)       
#define MODBUS_TX_PIN           (17)
// NVS 
#define WIFI_NVS_NAMESPACE   "wifi_config"
#define WIFI_SSID_KEY        "wifi_ssid"
#define WIFI_PASSWORD_KEY    "wifi_pass"
#define WIFI_SAVED_FLAG_KEY  "wifi_saved"


// GLOBAL VARIABLES
typedef struct{
    // Fixed variable per sensor
    uint8_t device_no;
    char device_name[20];
    // Variable as per register count
    char **register_Name;
    char **result;
    double *value;
}PAYLOAD;


typedef struct 
{
    // Fixed Variable per Sensor
    uint8_t device_no;
    uint8_t slave_id;
    uint8_t function_id;
    char Device_Name[20];
    uint8_t register_cnt;
// Variable as per the Register Count
    char     **Register_Name;
    uint64_t *Register_address;
    uint8_t *Quantity;
    uint8_t *data_type;
    uint8_t *byte_order;
    float   *offset;
    float   *slope;
    uint8_t *decimal;
}MODBUS_CONFIG;


typedef enum
{
    UINT8 = 1,
    INT8,

    UINT16,
    INT16,

    UINT32,
    INT32,
    FLOAT32,

    UINT48,
    INT48,
    
    UINT64,
    INT64,
    FLOAT64
    
} DATATYPE;


typedef enum
{
    BYTE_ORDER_ABCD = 1,
    BYTE_ORDER_BADC,
    BYTE_ORDER_CDAB,
    BYTE_ORDER_DCBA
} MODBUS_BYTE_ORDER;

static uint8_t modbus_response[64];
static PAYLOAD *payload;
static PAYLOAD *NEW_PAYLOAD = NULL;
static MODBUS_CONFIG *modbus_config;
static MODBUS_CONFIG *new_config = NULL;            // Temporary configuration
static uint8_t new_device_count = 0;
static uint8_t payload_device_count = 0;
static uint8_t ***Modbuscommand_array;
static uint8_t modbus_device_cnt;
static uint8_t modbus_config_f = false;
const uart_port_t uart_num = UART_NUM_1;
static uint8_t wifi_connect_f = 0;
static uint8_t mqtt_init_f = 0;
static char ssid[30];
static uint8_t wifi_retry_count = 0;
static char password[30];
static esp_mqtt_client_handle_t mqtt_client = NULL;
static char PAYLOAD_array[1024] = {0};
bool credentials_saved = false;
static char *mqtt_json_buffer = NULL;
static int mqtt_json_length = 0;
static int mqtt_json_total_length = 0;
static SemaphoreHandle_t config_mutex;



// GLOBAL FUNCTION DECLARATION
void free_modbus_config(MODBUS_CONFIG *config, size_t device_count);
esp_err_t clear_wifi_credentials(void);
esp_err_t load_wifi_credentials(char *ssid, size_t ssid_size, char *password,size_t password_size, bool *credentials_saved);
esp_err_t save_wifi_credentials(const char *ssid,const char *password);
static void publish_mqtt_payload(esp_mqtt_client_handle_t mqtt_client);
static void create_mqtt_payload(void);
static void Modbus_sensor_response(void);
static uint16_t Modbus_CRC16(const uint8_t *data, uint16_t length);
static void uart_init(void);
static void url_decode(char *str);
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data);
static void register_uri_handlers(httpd_handle_t server);
static esp_err_t my_uri_handler(httpd_req_t *req);
static esp_err_t wifi_post_handler(httpd_req_t *req);
static void start_webserver(void);
static void Modbus_command_generator(MODBUS_CONFIG *config, uint8_t count);
void free_payload(PAYLOAD *payload_ptr,  const MODBUS_CONFIG *config,size_t device_count);



void app_main(void)
{

config_mutex = xSemaphoreCreateMutex();
if (config_mutex == NULL)
{
    ESP_LOGE("CONFIG", "Mutex creation failed");
    return;
}    

esp_err_t ret = nvs_flash_init();

if (ret == ESP_ERR_NVS_NO_FREE_PAGES ||
    ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
{
    ESP_ERROR_CHECK(nvs_flash_erase());
    ret = nvs_flash_init();
}

// Load the WIFI - CREDENTIAL
esp_err_t err = load_wifi_credentials(ssid,sizeof(ssid), password,sizeof(password),&credentials_saved);
if (err != ESP_OK)
{
    ESP_LOGE("WI-FI", "Failed to read Wi-Fi configuration");
    credentials_saved = false;
}

    // 1) Initialize the TCP/IP stack and the event loop	
    esp_netif_init();
    esp_event_loop_create_default();

    esp_netif_create_default_wifi_ap();
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&cfg); // wi-fi driver initialize


if (credentials_saved)
{
    ESP_LOGI("WI-FI", "Credentials available. Connecting to Wi-Fi");
    wifi_config_t sta_config = {0};
    strcpy((char *)sta_config.sta.ssid,ssid); // The updated SSID is put to the structure variable taken from the user
    strcpy((char *)sta_config.sta.password,password); // The updated Password on the structure variable takem from the user

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));// Mode changed to the statinary mode
    esp_wifi_set_config(WIFI_IF_STA, &sta_config);
    esp_wifi_start();// start the wi-fi 
    esp_wifi_connect(); // start to connect to the wifi- router    
}
else
{

    wifi_config_t ap_config = {
        .ap = {
            .ssid = "ESP32 IOT",
            .password = "123456789",
            .channel = 1,
            .max_connection = 4,
        },
    };

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    esp_wifi_set_config(WIFI_IF_AP, &ap_config);
    esp_wifi_start();// start the wi-fi 
    // Start the HTTP WEB SERVER 
    start_webserver();
}

// UART INIT
uart_init();
// Event Handler
ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID,&wifi_event_handler, NULL, NULL)); // WI-FI EVENT HANDLER CONFIGURED
ESP_ERROR_CHECK(esp_event_handler_register( IP_EVENT,IP_EVENT_STA_GOT_IP, &wifi_event_handler,NULL)); // IP EVENT HANDLER IS CONFIGURED


        while (true) 
        {
            if (wifi_connect_f == true)
            {
                // Initialize MQTT
                    if (!mqtt_init_f)
                    {
                        const esp_mqtt_client_config_t mqtt_cfg = {
                            .broker = {
                                .address.uri = MQTT_BROKER_URI,
                                .verification.skip_cert_common_name_check = true,
                                .verification.crt_bundle_attach = esp_crt_bundle_attach,
                            },

                            .network = {
                                .timeout_ms = 30000,
                                .reconnect_timeout_ms = 10000,
                            },

                            .credentials = {
                                .username = MQTT_USER,
                                .authentication.password = MQTT_PASSWORD,
                            },

                            .session = {
                                .last_will.topic = DEVICE_STATUS_TOPIC,
                                .last_will.msg = "{\"status\":\"Inactive\"}",
                                .last_will.msg_len =
                                    sizeof("{\"status\":\"Inactive\"}") - 1,
                                .last_will.qos = 1,
                                .last_will.retain = 0,
                                .keepalive = 60,
                            },
                        };

                        // Keep your existing client initialization and start logic.

                    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
                    esp_mqtt_client_register_event(
                        mqtt_client,
                        ESP_EVENT_ANY_ID,
                        mqtt_event_handler,
                        mqtt_client
                    );

                    esp_mqtt_client_start(mqtt_client);

                    mqtt_init_f = true;
                }
            }

            // MODBUS COMMUNICATION 
            if (modbus_config_f == true)
            {
                ESP_LOGI("MAIN", "Taking config mutex");

                xSemaphoreTake(config_mutex, portMAX_DELAY);

                ESP_LOGI("MAIN", "Config mutex acquired");

                if (modbus_config == NULL)
                {
                    ESP_LOGE("MAIN", "modbus_config is NULL");

                    xSemaphoreGive(config_mutex);
                    return;
                }

                ESP_LOGI("MAIN", "Calling Modbus_sensor_response()");
                Modbus_sensor_response();
                xSemaphoreGive(config_mutex);
                ESP_LOGI("MAIN", "Calling publish_mqtt_payload()");
                publish_mqtt_payload(mqtt_client);
                ESP_LOGI("MAIN", "publish_mqtt_payload() completed");
            }
            vTaskDelay(pdMS_TO_TICKS(10000));
       }
}


// GLOBAL FUNCTION DEFINATION
static void uart_init(void)
{
    /* MODBUS PROTOCOL UART INIT */
    uart_config_t uart_config = {
    .baud_rate = 9600,
    .data_bits = UART_DATA_8_BITS,
    .parity = UART_PARITY_DISABLE,
    .stop_bits = UART_STOP_BITS_1,
    .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
    };
    // Configure UART parameters
    ESP_ERROR_CHECK(uart_param_config(uart_num, &uart_config));
    ESP_ERROR_CHECK(uart_set_pin(uart_num,MODBUS_TX_PIN, MODBUS_RX_PIN, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE));
    ESP_ERROR_CHECK(uart_driver_install(uart_num,1024,1024,0, NULL,0));
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data)
{
esp_mqtt_event_handle_t event = event_data;
esp_mqtt_client_handle_t mqtt_client = event->client;

    switch(event_id)
    {
        case MQTT_EVENT_CONNECTED:
        ESP_LOGI("MQTT","CONNECTED");
        // One time Published to a topic to tell about the status of the device is it active or inactive
        char message[] = "{\"status\":\"Active\"}";
        esp_mqtt_client_publish(mqtt_client,DEVICE_STATUS_TOPIC,message,strlen(message),1,0); // no retain and atleast once send to the broker
        // Here we subscribe to a Topic for Getting the configuration
        esp_mqtt_client_subscribe(mqtt_client,SUBSCRIBE_TOPIC,1);
        break;

        case MQTT_EVENT_DISCONNECTED:
        ESP_LOGI("MQTT","DISCONNECTED");
        break;

        case MQTT_EVENT_PUBLISHED:
        ESP_LOGI("MQTT","PUBLISHED PAYLOAD");
        break;

        case MQTT_EVENT_SUBSCRIBED:
        ESP_LOGI("MQTT","SUBSCRIBED DATA");  
        break;

    case MQTT_EVENT_DATA:
    ESP_LOGI("MQTT", "SUBSCRIBED TOPIC DATA RECEIVED");

    if (event->current_data_offset == 0)
    {
        free(mqtt_json_buffer);
        mqtt_json_buffer = NULL;

        mqtt_json_length = 0;
        mqtt_json_total_length = event->total_data_len;

        if (mqtt_json_total_length <= 0)
        {
            ESP_LOGE("MQTT", "Invalid total data length");
            break;
        }

        mqtt_json_buffer = calloc(
            (size_t)mqtt_json_total_length + 1,
            sizeof(char));

        if (mqtt_json_buffer == NULL)
        {
            ESP_LOGE("MQTT", "Memory allocation failed");
            mqtt_json_total_length = 0;
            break;
        }

        printf("Topic = %.*s\n",
               event->topic_len,
               event->topic);
    }

    if (mqtt_json_buffer == NULL ||
        event->current_data_offset != mqtt_json_length ||
        event->data_len < 0 ||
        mqtt_json_length + event->data_len > mqtt_json_total_length)
    {
        ESP_LOGE("MQTT", "Invalid MQTT data fragment");

        free(mqtt_json_buffer);
        mqtt_json_buffer = NULL;
        mqtt_json_length = 0;
        mqtt_json_total_length = 0;
        break;
    }

    memcpy(mqtt_json_buffer + mqtt_json_length,
           event->data,
           event->data_len);

    mqtt_json_length += event->data_len;

    ESP_LOGI("MQTT", "Received %d/%d bytes",
             mqtt_json_length,
             mqtt_json_total_length);

    // Wait for the complete JSON payload
    if (mqtt_json_length < mqtt_json_total_length)
    {
        break;
    }

    mqtt_json_buffer[mqtt_json_length] = '\0';

    printf("Complete Data = %s\n", mqtt_json_buffer);

    // JSON PARSING
    cJSON *json = cJSON_ParseWithLength(
        mqtt_json_buffer,
        (size_t)mqtt_json_length);

    if (json == NULL)
    {
        const char *error_ptr = cJSON_GetErrorPtr();

        ESP_LOGE("JSON", "JSON parsing failed near: %.40s",
                 error_ptr ? error_ptr : "unknown");

        free(mqtt_json_buffer);
        mqtt_json_buffer = NULL;
        mqtt_json_length = 0;
        mqtt_json_total_length = 0;
        break;
    }
    // --------------------------------------------------
    // GET DEVICE COUNT
    // --------------------------------------------------

    cJSON *device_count =
        cJSON_GetObjectItemCaseSensitive(json, "Device_count");

    if (!cJSON_IsNumber(device_count))
    {
        printf("ERROR: Device_count is not a number\n");
        cJSON_Delete(json);
        return;
    }

    new_device_count = device_count->valueint;

    printf("Device count = %d\n", new_device_count);


    // --------------------------------------------------
    // GET DEVICES ARRAY
    // --------------------------------------------------

    cJSON *devices =
        cJSON_GetObjectItemCaseSensitive(json, "Devices");

    if (!cJSON_IsArray(devices))
    {
        printf("ERROR: Devices is not an array\n");
        cJSON_Delete(json);
        return;
    }


    // Check actual number of devices
    int array_count = cJSON_GetArraySize(devices);

    if (array_count != new_device_count)
    {
        printf("ERROR: Device count mismatch\n");
        printf("Device_count = %d, Array count = %d\n",
               new_device_count,
               array_count);

        cJSON_Delete(json);
        return;
    }


    // --------------------------------------------------
    // ALLOCATE MEMORY
    // --------------------------------------------------
    if (new_config  != NULL)
    {
        free_modbus_config(new_config,new_device_count);
        new_config = NULL;
    }
    new_config = calloc(new_device_count,sizeof(MODBUS_CONFIG)); // it is also need to be free

    NEW_PAYLOAD = calloc(new_device_count,sizeof(PAYLOAD)); // as per the number of Device the result also vary and we have to free the space after sending the payload 

    if (NEW_PAYLOAD == NULL)
    {
        printf("ERROR: Memory allocation failed\n");
        cJSON_Delete(json);
        return;
    }
    if (new_config == NULL)
    {
        printf("ERROR: Memory allocation failed\n");
        cJSON_Delete(json);
        return;
    }

    // --------------------------------------------------
    // PARSE EACH DEVICE
    // --------------------------------------------------

    for (uint8_t index = 0;  index < new_device_count; index++)
    {
        cJSON *device = cJSON_GetArrayItem(devices, index);

        if (!cJSON_IsObject(device))
        {
            printf("ERROR: Device %d is not an object\n",index);
            if (new_config != NULL)
            {
                free_modbus_config(new_config,new_device_count);
                new_config = NULL;
            }
            cJSON_Delete(json);
            return;
        }


        // --------------------------------------------------
        // Device_no
        // --------------------------------------------------

        cJSON *device_no = cJSON_GetObjectItemCaseSensitive(device,"Device_no");
        if (!cJSON_IsNumber(device_no))
        {
            printf("ERROR: Device_no invalid\n");
            if (new_config != NULL)
            {
                free_modbus_config(new_config,new_device_count);
                new_config = NULL;
            }
            cJSON_Delete(json);
            return;
        }

        new_config[index].device_no = device_no->valueint;




        // --------------------------------------------------
        // DEVICE Name.
        // --------------------------------------------------

        cJSON *device_name = cJSON_GetObjectItemCaseSensitive(device, "Device_Name");

        if (!cJSON_IsString(device_name) ||
            device_name->valuestring == NULL)
        {
            printf("ERROR: Device Name is invalid\n");
            if (new_config != NULL)
            {
                free_modbus_config(new_config,new_device_count);
                new_config = NULL;
            }
            cJSON_Delete(json);
            return;
        }
        snprintf(new_config[index].Device_Name,sizeof(new_config[index].Device_Name),"%s",device_name->valuestring);

        // --------------------------------------------------
        // Slave_ID
        // --------------------------------------------------

        cJSON *slave_id = cJSON_GetObjectItemCaseSensitive(device,
                                             "Slave_ID");

        if (!cJSON_IsString(slave_id) ||
            slave_id->valuestring == NULL)
        {
                printf("ERROR: Slave_ID invalid\n");
            if (new_config != NULL)
            {
                free_modbus_config(new_config,new_device_count);
                new_config = NULL;
            }
            cJSON_Delete(json);
            return;
        }

        new_config[index].slave_id =
            (uint8_t)strtol(slave_id->valuestring,
                            NULL,
                            0);


        // --------------------------------------------------
        // Function_ID
        // --------------------------------------------------

        cJSON *function_id = cJSON_GetObjectItemCaseSensitive(device,
                                             "Function_ID");

        if (!cJSON_IsString(function_id) ||
            function_id->valuestring == NULL)
        {
            printf("ERROR: Function_ID invalid\n");

            if (new_config != NULL)
            {
                free_modbus_config(new_config,new_device_count);
                new_config = NULL;
            }

            cJSON_Delete(json);
            return;
        }

        new_config[index].function_id =
            (uint8_t)strtol(function_id->valuestring,
                            NULL,
                            0);





        // --------------------------------------------------
        // Register_Count
        // -----------------------------------------------
        cJSON *register_cnt = cJSON_GetObjectItemCaseSensitive(device, "Register_Count");
        if (!cJSON_IsNumber(register_cnt))
        {
            printf("ERROR: Register Count invalid\n");

            if (new_config != NULL)
            {
            free_modbus_config(new_config,new_device_count);
            new_config = NULL;
            }

            cJSON_Delete(json);
            return;
        }

        new_config[index].register_cnt = register_cnt->valueint;



        // Variable Values
        new_config[index].Register_Name =calloc(new_config[index].register_cnt, sizeof(*new_config[index].Register_Name));
        new_config[index].Register_address = calloc(new_config[index].register_cnt,sizeof(uint64_t));
        new_config[index].Quantity = calloc(new_config[index].register_cnt,sizeof(uint8_t)); 
        new_config[index].data_type = calloc(new_config[index].register_cnt,sizeof(uint8_t));
        new_config[index].byte_order = calloc(new_config[index].register_cnt,sizeof(uint8_t));
        new_config[index].offset = calloc(new_config[index].register_cnt,sizeof(float));
        new_config[index].slope = calloc(new_config[index].register_cnt,sizeof(float));
        new_config[index].decimal = calloc(new_config[index].register_cnt,sizeof(uint8_t));
       // Payload
        NEW_PAYLOAD[index].register_Name = calloc(new_config[index].register_cnt, sizeof(*NEW_PAYLOAD[index].register_Name));
        NEW_PAYLOAD[index].result = calloc(new_config[index].register_cnt, sizeof(*NEW_PAYLOAD[index].result));
        NEW_PAYLOAD[index].value =calloc(new_config[index].register_cnt, sizeof(*NEW_PAYLOAD[index].value));


        for(uint8_t j = 0 ; j < new_config[index].register_cnt ; j++)
        {
                    // --------------------------------------------------
                    // Register_Name
                    // --------------------------------------------------
                 new_config[index].Register_Name[j] = calloc(64, sizeof(char));

                NEW_PAYLOAD[index].register_Name[j] =calloc(64, sizeof(char));

                NEW_PAYLOAD[index].result[j] = calloc(10, sizeof(char));

                if (new_config[index].Register_Name[j] == NULL ||
                    NEW_PAYLOAD[index].register_Name[j] == NULL ||
                    NEW_PAYLOAD[index].result[j] == NULL)
                {
                    ESP_LOGE("CONFIG",
                            "Register string allocation failed at index %u",
                            (unsigned)j);
                    return;
                }

                    cJSON *register_name_array =cJSON_GetObjectItemCaseSensitive(device,"Register_Name");

                    if(cJSON_IsArray(register_name_array))
                    {
                        // Get the element: Ex - "ORP"
                        cJSON *register_item = cJSON_GetArrayItem(register_name_array, j);
                        if (cJSON_IsString(register_item) && register_item->valuestring != NULL)
                        {
                          new_config[index].Register_Name[j] =   strdup(register_item->valuestring);
                        }
                        else
                        {
                            printf("ERROR: Register_Name invalid\n");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;
                        }
                    }
                    else
                    {
                        printf("ERROR: Start_Address invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }

                    // --------------------------------------------------
                    // Register_Address
                    // --------------------------------------------------

                    cJSON *register_address_array =cJSON_GetObjectItemCaseSensitive(device,"Register_Address");
                    if (cJSON_IsArray(register_address_array))
                    {
                        // Get the register address at index j
                        cJSON *address_item = cJSON_GetArrayItem(register_address_array, j);

                        if (cJSON_IsString(address_item) && address_item->valuestring != NULL)
                        {
                            char *endptr = NULL;

                            unsigned long long address = strtoull(address_item->valuestring, &endptr, 0);

                            if (endptr != address_item->valuestring &&*endptr == '\0')
                            {
                                new_config[index].Register_address[j] = (uint64_t)address;
                            }
                            else
                            {
                                ESP_LOGE("MODBUS", "Invalid register address: %s",
                                        address_item->valuestring);
                            }
                        }
                        else
                        {
                            ESP_LOGE("MODBUS", "Register address is not a string");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;
                        }
                    }
                    else
                    {
                        printf("ERROR: Register_Address invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }


                    // --------------------------------------------------
                    // Quantity
                    // --------------------------------------------------
                    cJSON *quantity_array = cJSON_GetObjectItemCaseSensitive(device,"Quantity");

                    if(cJSON_IsArray(quantity_array))
                    {
                        // Get the element
                        cJSON *quantity_item = cJSON_GetArrayItem(quantity_array, j);
                        if (cJSON_IsNumber(quantity_item))
                        {
                            new_config[index].Quantity[j] = quantity_item->valueint;
                        }
                        else
                        {
                            printf("ERROR: Quantity invalid\n");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;
                        }
                    }
                    else
                    {
                        printf("ERROR: Quantity invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }
                    
                    // --------------------------------------------------
                    // Data_Type
                    // --------------------------------------------------

                    cJSON *data_type_array = cJSON_GetObjectItemCaseSensitive(device, "Data_Type");

                    if(cJSON_IsArray(data_type_array))
                    {
                        cJSON *data_type_item = cJSON_GetArrayItem(data_type_array, j);

                        if (cJSON_IsString(data_type_item) && data_type_item->valuestring != NULL)
                        {
                                if (strcmp(data_type_item->valuestring, "uint8") == 0)
                                {
                                    new_config[index].data_type[j] = 1;
                                }
                                else if (strcmp(data_type_item->valuestring, "int8") == 0)
                                {
                                    new_config[index].data_type[j] = 2;
                                }
                                else if (strcmp(data_type_item->valuestring, "uint16") == 0)
                                {
                                    new_config[index].data_type[j] = 3;
                                }
                                else if (strcmp(data_type_item->valuestring, "int16") == 0)
                                {
                                    new_config[index].data_type[j] = 4;
                                }
                                else if (strcmp(data_type_item->valuestring, "uint32") == 0)
                                {
                                    new_config[index].data_type[j] = 5;
                                }
                                else if (strcmp(data_type_item->valuestring, "int32") == 0)
                                {
                                    new_config[index].data_type[j] = 6;
                                }
                                else if (strcmp(data_type_item->valuestring, "float32") == 0)
                                {
                                    new_config[index].data_type[j] = 7;
                                }
                                else if (strcmp(data_type_item->valuestring, "uint64") == 0)
                                {
                                    new_config[index].data_type[j] = 8;
                                }                
                                else if (strcmp(data_type_item->valuestring, "int64") == 0)
                                {
                                    new_config[index].data_type[j] = 9;
                                }        
                                else if (strcmp(data_type_item->valuestring, "float64") == 0)
                                {
                                    new_config[index].data_type[j] = 10;
                                }        
                                else
                                {
                                    printf("ERROR: Unknown Data_Type = %s\n",data_type_item->valuestring);
                                    if (new_config != NULL)
                                    {
                                        free_modbus_config(new_config,new_device_count);
                                        new_config = NULL;
                                    }
                                    cJSON_Delete(json);
                                    return;
                                }                           
                        }
                        else
                        {
                            printf("ERROR: Data Type invalid\n");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;
                        }
                    }
                    else
                    {
                        printf("ERROR: Data Type invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    } 

                    // --------------------------------------------------
                    // Byte_Order
                    // --------------------------------------------------

                    cJSON *byte_order_array = cJSON_GetObjectItemCaseSensitive(device,"Byte_Order");

                     if(cJSON_IsArray(byte_order_array))
                     {
                        cJSON *byte_order_item = cJSON_GetArrayItem(byte_order_array, j);

                        if (cJSON_IsString(byte_order_item) && byte_order_item->valuestring != NULL)
                        {
                            if (strcmp(byte_order_item->valuestring, "ABCD") == 0)
                            {
                                new_config[index].byte_order[j] = 1;
                            }
                            else if (strcmp(byte_order_item->valuestring, "BADC") == 0)
                            {
                                new_config[index].byte_order[j] = 2;
                            }
                            else if (strcmp(byte_order_item->valuestring, "CDAB") == 0)
                            {
                                new_config[index].byte_order[j] = 3;
                            }
                            else if (strcmp(byte_order_item->valuestring, "DCBA") == 0)
                            {
                                new_config[index].byte_order[j] = 4;
                            }
                            else
                            {
                                printf("ERROR: Unknown Byte_Order = %s\n",byte_order_item->valuestring);
                                if (new_config != NULL)
                                {
                                    free_modbus_config(new_config,new_device_count);
                                    new_config = NULL;
                                }
                                cJSON_Delete(json);
                                return;
                            }
                        }
                        else
                        {
                                printf("Byte Order is Invalid\n");
                                if (new_config != NULL)
                                {
                                    free_modbus_config(new_config,new_device_count);
                                    new_config = NULL;
                                }
                                cJSON_Delete(json);
                                return;                            
                        }
                    }
                    else
                    {
                        printf("Byte Order is Invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }                    
                    // --------------------------------------------------
                    // Offset
                    // --------------------------------------------------

                    cJSON *offset_array =cJSON_GetObjectItemCaseSensitive(device,"Offset");

                    if(cJSON_IsArray(offset_array))
                    {
                        cJSON *offset_item = cJSON_GetArrayItem(offset_array, j);

                        if(cJSON_IsNumber(offset_item))
                        {
                             new_config[index].offset[j] = offset_item->valuedouble;
                        }
                        else
                        {
                            printf("Offset is Invalid\n");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;                            
                        }
                    }
                    else
                    {
                        printf("Offset is Invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;                            
                    }

                    // --------------------------------------------------
                    // Slope
                    // --------------------------------------------------

                    cJSON *Slope_array =cJSON_GetObjectItemCaseSensitive(device, "Slope");

                    if(cJSON_IsArray(Slope_array))
                    {   
                        cJSON *slope_item = cJSON_GetArrayItem(Slope_array, j);
                        if(cJSON_IsNumber(slope_item))
                        {
                             new_config[index].slope[j] = slope_item->valuedouble;
                        }
                        else
                        {
                            printf("Slope is Invalid\n");
                            if (new_config != NULL)
                            {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;                             
                        }
                    }
                    else
                    {
                        printf("ERROR: Scale invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }


                    // --------------------------------------------------
                    // Decimal
                    // --------------------------------------------------

                    cJSON *decimal_array = cJSON_GetObjectItemCaseSensitive(device,"Decimal");

                    if(cJSON_IsArray(decimal_array))
                    {
                        cJSON *decimal_item = cJSON_GetArrayItem(decimal_array, j);
                        if(cJSON_IsNumber(decimal_item))
                        {
                            new_config[index].decimal[j] = decimal_item->valueint;
                        }
                        else
                        {
                            printf("ERROR:Decimal invalid\n");
                            if (new_config != NULL)
                            {
                                free_modbus_config(new_config,new_device_count);
                                new_config = NULL;
                            }
                            cJSON_Delete(json);
                            return;
                        }
                    }
                    else
                    {
                        printf("ERROR:Decimal invalid\n");
                        if (new_config != NULL)
                        {
                            free_modbus_config(new_config,new_device_count);
                            new_config = NULL;
                        }
                        cJSON_Delete(json);
                        return;
                    }
                    // --------------------------------------------------
                    // PRINT RESULT
                    // --------------------------------------------------

                    printf("\nDevice %d\n", index + 1);

                    printf("Device No     = %d\n", new_config[index].device_no);

                    printf("Slave ID      = 0x%02X\n", new_config[index].slave_id);

                    printf("Function ID   = 0x%02X\n",new_config[index].function_id);

                    // Variable 
                    printf("Register Name = %s\n, index = %d", new_config[index].Register_Name[j],j);    
                        
                    printf("Register Address = 0x%04X\n , index = %d",(unsigned int)new_config[index].Register_address[j],j);

                    printf("Quantity = %d\n, index = %d\n", new_config[index].Quantity[j],j);

                    printf("Data Type     = %d\n , index = %d\n", new_config[index].data_type[j],j);

                    printf("Byte Order    = %d\n , index = %d\n",new_config[index].byte_order[j],j);

                    printf("Offset        = %.2f\n , index = %d\n",new_config[index].offset[j],j);

                    printf("Scale         = %.2f\n , index = %d\n", new_config[index].slope[j],j);

                    printf("Decimal  = %d\n , index = %d\n", new_config[index].decimal[j],j);
        }
}
    xSemaphoreTake(config_mutex, portMAX_DELAY);
    // Save the existing active pointers
    MODBUS_CONFIG *old_config = modbus_config;
    uint8_t old_count = modbus_device_cnt;

    PAYLOAD *old_payload = payload;
    uint8_t old_payload_count = payload_device_count;

    // Activate the new configuration and payload
    modbus_config = new_config;
    modbus_device_cnt = new_device_count;

    payload = NEW_PAYLOAD;
    payload_device_count = new_device_count;

    // Transfer ownership to the active pointers
    new_config = NULL;
    NEW_PAYLOAD = NULL;

    // Generate commands for the new configuration
    Modbus_command_generator(modbus_config, modbus_device_cnt);

    xSemaphoreGive(config_mutex);

    // Free the old allocations
    free_modbus_config(old_config, old_count);
    free_payload(old_payload, old_config, old_payload_count);

    // --------------------------------------------------
    // DELETE JSON
    // --------------------------------------------------
    cJSON_Delete(json);
    break;

    case MQTT_EVENT_ERROR:
    ESP_LOGI("MQTT","ERROR");
    break;

    default:
    ESP_LOGI("MQTT","DEFAULT CASE EXECUTE");
    break;
    }
}

static void Modbus_command_generator(MODBUS_CONFIG *config, uint8_t count)
{
    bool command_generated_f = true;

    if (config == NULL || count == 0)
    {
        return;
    }

    /* Allocate memory for all devices */
    Modbuscommand_array = calloc(count, sizeof(*Modbuscommand_array));

    if (Modbuscommand_array == NULL)
    {
        return;
    }

    for (size_t index = 0; index < count; index++)
    {
        if (config[index].register_cnt == 0 ||
            config[index].Register_address == NULL ||
            config[index].Quantity == NULL)
        {
            command_generated_f = false;
            break;
        }

        /* Allocate the register-command pointer array */
        Modbuscommand_array[index] =
            calloc(config[index].register_cnt,
                   sizeof(*Modbuscommand_array[index]));

        if (Modbuscommand_array[index] == NULL)
        {
            command_generated_f = false;
            break;
        }

        for (size_t j = 0;  j < config[index].register_cnt; j++)
        {
            /* Allocate an 8-byte Modbus RTU command */
            Modbuscommand_array[index][j] = calloc(8, sizeof(uint8_t));

            if (Modbuscommand_array[index][j] == NULL)
            {
                command_generated_f = false;
                break;
            }

            /* Slave ID */
            Modbuscommand_array[index][j][0] =config[index].slave_id;

            /* Function code */
            Modbuscommand_array[index][j][1] = config[index].function_id;

            /* Starting register address */
            Modbuscommand_array[index][j][2] = (config[index].Register_address[j] >> 8) & 0xFF;

            Modbuscommand_array[index][j][3] = config[index].Register_address[j] & 0xFF;

            /* Register quantity */
            Modbuscommand_array[index][j][4] = (config[index].Quantity[j] >> 8) & 0xFF;

            Modbuscommand_array[index][j][5] = config[index].Quantity[j] & 0xFF;

            /* CRC16: low byte first */
            uint16_t temp_crc = Modbus_CRC16(Modbuscommand_array[index][j], 6);

            Modbuscommand_array[index][j][6] = temp_crc & 0xFF;

            Modbuscommand_array[index][j][7] = (temp_crc >> 8) & 0xFF;

            /* Print Modbus command */
            printf("\nDevice %u, Register %u - Modbus Command: ",
                   (unsigned int)(index + 1),
                   (unsigned int)(j + 1));

            for (uint8_t k = 0; k < 8; k++)
            {
                printf("%02X ", Modbuscommand_array[index][j][k]);
            }

            printf("\n");

        }
    }

   if(command_generated_f == true)
   {
        modbus_config_f = true;
        return;
   } 
   else
   {
        /* Free all successfully allocated command buffers */
        for (size_t i = 0; i < count; i++)
        {
            if (Modbuscommand_array[i] != NULL)
            {
                for (size_t j = 0;
                    j < config[i].register_cnt;
                    j++)
                {
                    free(Modbuscommand_array[i][j]);
                }

                free(Modbuscommand_array[i]);
            }
        }

        free(Modbuscommand_array);
        Modbuscommand_array = NULL;
        modbus_config_f = false;
   }
}
static uint16_t Modbus_CRC16(const uint8_t *data, uint16_t length)
{
    uint16_t crc = 0xFFFF;

    for (uint16_t i = 0; i < length; i++)
    {
        crc ^= data[i];

        for (uint8_t bit = 0; bit < 8; bit++)
        {
            if (crc & 0x0001)
            {
                crc = (crc >> 1) ^ 0xA001;
            }
            else
            {
                crc >>= 1;
            }
        }
    }

    return crc;
}

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if(event_id == WIFI_EVENT_AP_STACONNECTED)
    {
        ESP_LOGI("WIFI","AP CONNECTED");
    }
    else if(event_id == WIFI_EVENT_AP_START)
    {
        ESP_LOGI("WIFI","AP STARTED");
    }
    else if(event_id == WIFI_EVENT_AP_STOP)
    {
        ESP_LOGI("WIFI","AP STOP");
    }
    else if(event_id == WIFI_EVENT_AP_STADISCONNECTED)
    {
        ESP_LOGI("WIFI","AP DISCONNECTED");
    }
    // Stationary Mode Event 
    else if(event_id == WIFI_EVENT_STA_CONNECTED)
    {
        ESP_LOGI("WIFI","STAT CONNECTED");
    }
    else if(event_id == WIFI_EVENT_STA_DISCONNECTED)
    {       
        ESP_LOGI("WIFI","STAT DISCONNECTED");
        wifi_retry_count++;
        ESP_LOGI("WIFI:","DISCONNECT COUNT = %d",wifi_retry_count);
        wifi_connect_f = false;
        if(wifi_retry_count >= WIFI_RETRY_THRESHOLD)
        {
            // WIFT NODE CHANGE TO ACCESS POINT 
            clear_wifi_credentials(); // clear the saved crendential
            wifi_retry_count = 0; // reset the wifi retry counter 
            esp_wifi_stop();
            wifi_config_t ap_config = {
                .ap = {
                    .ssid = "ESP32 IOT",
                    .password = "123456789",
                    .channel = 1,
                    .max_connection = 4,
                },
            };
            ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
            esp_wifi_set_config(WIFI_IF_AP, &ap_config);
            esp_wifi_start();// start the wi-fi i 
            start_webserver();
        }
        else
        {
            // Retry WiFi connection
            esp_wifi_connect();
        }
    } 
    else if (event_base == IP_EVENT &&
        event_id == IP_EVENT_STA_GOT_IP)
    {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;

        ESP_LOGI("WIFI", "Got IP: " IPSTR,
                IP2STR(&event->ip_info.ip));
        wifi_connect_f = true;
    }     
}
void start_webserver(void)
{
    // Generate default configuration
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    // Empty handle to http_server
    httpd_handle_t server = NULL;

    if (httpd_start(&server, &config) == ESP_OK)
    {
        register_uri_handlers(server);
    }
}

void register_uri_handlers(httpd_handle_t server)
{
    httpd_uri_t my_uri = {
        .uri       = "/",
        .method    = HTTP_GET,
        .handler   = my_uri_handler,
        .user_ctx  = NULL
    };

    // Post Request 
        httpd_uri_t wifi_post_uri  = {
        .uri = "/wifi",
        .method = HTTP_POST,
        .handler = wifi_post_handler,
        .user_ctx = NULL
    };

    httpd_register_uri_handler(server, &my_uri);

    httpd_register_uri_handler(server, &wifi_post_uri);

}
static esp_err_t wifi_post_handler(httpd_req_t *req)
{
    char buffer[512];
    ESP_LOGI("HTTP","POST REQUEST EXECUTE");
    int len = httpd_req_recv(req,buffer,sizeof(buffer));
    if(len == HTTPD_SOCK_ERR_INVALID)
    {
        ESP_LOGI("POST:","ERROR");
    }
    else if(len ==HTTPD_SOCK_ERR_TIMEOUT)
    {
        ESP_LOGI("POST:","TIMEOUT ERROR");
    }
    else if(len == HTTPD_SOCK_ERR_FAIL)
    {
        ESP_LOGI("POST:","FAIL");
    }

    if (len > 0) {
        buffer[len] = '\0';
        ESP_LOGI("HTTP", "POST data: %s", buffer);
    }
    
    char *start = strstr(buffer, "ssid=");
    if (start != NULL)
    {
        start += strlen("ssid=");

        char *end = strchr(start, '&');

        if (end != NULL)
        {
            size_t len = end - start;

            if (len < sizeof(ssid))
            {
                memcpy(ssid, start, len);
                ssid[len] = '\0'; // Wifi SSID is getting stored on this array
            }
        }
    }
    else
    {
        char *error_str = "ERROR !!!";
        httpd_resp_sendstr(req,error_str);
        return(ESP_FAIL);
    }

    // Decoding the URL Receive from HTTP 
    url_decode(ssid);

    char *startP = strstr(buffer, "password=");
    if (startP != NULL)
    {
        startP += strlen("password=");

        size_t len = strlen(startP);

        if (len < sizeof(password))
        {
            memcpy(password, startP, len);
            password[len] = '\0';  // Wifi Password is Getting stored on this array
        }
    }
    else
    {
        char *error_str = "ERROR !!!";
        httpd_resp_sendstr(req,error_str);
        return(ESP_FAIL);
    }

    // Decoding the URL Receive from HTTP 
    url_decode(password);

    /*********************** Debug **************************/
    // Print the ssid and Password
    char ch = ssid[0];
    uint8_t index = 0;
    printf("SSID given by the users : ");
    while(ch != '\0')
    {
        printf("%c",ch);
        index++;
        ch = ssid[index];
    }
    printf("\n");

    ch = password[0];
    index = 0;
    printf("Password given by the users : ");
    while(ch != '\0')
    {
        printf("%c",ch);
        index++;
        ch = password[index];
    }
    printf("\n");

    char *resp_str = "Successfull";
    httpd_resp_sendstr(req,resp_str);

/************* WIFI MODE CHANGED FROM ACCESS POINT TO THE STATIONARY ******************/
esp_wifi_stop();
wifi_config_t sta_config = {0};
strcpy((char *)sta_config.sta.ssid,ssid); // The updated SSID is put to the structure variable taken from the user
strcpy((char *)sta_config.sta.password,password); // The updated Password on the structure variable takem from the user
esp_err_t err = save_wifi_credentials(ssid,password); // save the credenetial into the NVS flash 
if(err == ESP_OK)
{
   printf("WIFI PASSWORD SAVED\n");
}
else
{
    printf("WIFI PASSWORD NOT SAVED\n");
}
ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));// Mode changed to the statinary mode
esp_wifi_set_config(WIFI_IF_STA, &sta_config);
esp_wifi_start();// start the wi-fi 
esp_wifi_connect(); // start to connect to the wifi- router 
return ESP_OK;
}
static esp_err_t my_uri_handler(httpd_req_t *req)
{

    ESP_LOGI("HTTP", "GET REQUEST EXECUTED");

 const char *html =
        "<!DOCTYPE html>"
        "<html>"
        "<head>"
        "<title>ESP32 Wi-Fi Setup</title>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"

        "<style>"
        "body {"
        "font-family: Arial, sans-serif;"
        "background: #f2f2f2;"
        "text-align: center;"
        "margin-top: 50px;"
        "}"

        ".container {"
        "background: white;"
        "width: 320px;"
        "margin: auto;"
        "padding: 25px;"
        "border-radius: 10px;"
        "}"

        "input {"
        "width: 90%;"
        "padding: 12px;"
        "margin: 10px 0;"
        "font-size: 16px;"
        "}"

        "input[type=\"submit\"] {"
        "width: 98%;"
        "background: #4CAF50;"
        "color: white;"
        "border: none;"
        "cursor: pointer;"
        "}"
        "</style>"

        "</head>"

        "<body>"

        "<div class=\"container\">"

        "<h2>ESP32 Wi-Fi Setup</h2>"

        "<form action=\"/wifi\" method=\"POST\">"

        "<input "
        "type=\"text\" "
        "name=\"ssid\" "
        "placeholder=\"Enter Wi-Fi SSID\" "
        "required>"

        "<br>"

        "<input "
        "type=\"password\" "
        "name=\"password\" "
        "placeholder=\"Enter Wi-Fi Password\" "
        "required>"

        "<br><br>"

        "<input "
        "type=\"submit\" "
        "value=\"Connect\">"

        "</form>"

        "</div>"

        "</body>"
        "</html>";

    httpd_resp_set_type(req, "text/html"); // http content type is html     

    httpd_resp_send(req, html, HTTPD_RESP_USE_STRLEN); // send the http response

    return ESP_OK;
}

static void url_decode(char *str)
{
    char *src = str;
    char *dst = str;

    while (*src)
    {
        if (*src == '+')
        {
            *dst++ = ' ';
            src++;
        }
        else if (*src == '%' &&
                 isxdigit((unsigned char)src[1]) &&
                 isxdigit((unsigned char)src[2]))
        {
            char hex[3] = {src[1], src[2], '\0'};
            *dst++ = (char)strtol(hex, NULL, 16);
            src += 3;
        }
        else
        {
            *dst++ = *src++;
        }
    }
    *dst = '\0';
}
static void Modbus_sensor_response(void)
{
    if (modbus_device_cnt == 0 ||
    modbus_config == NULL ||
    Modbuscommand_array == NULL ||
    payload == NULL)
    {
        ESP_LOGE("MODBUS", "Invalid configuration or NULL pointer");
        return;
    }
    for (uint8_t index = 0; index < modbus_device_cnt; index++)
    {

        if (modbus_config[index].register_cnt == 0 ||
        modbus_config[index].Register_Name == NULL ||
        Modbuscommand_array[index] == NULL ||
        payload[index].register_Name == NULL ||
        payload[index].result == NULL ||
        payload[index].value == NULL)
        {
            ESP_LOGE("MODBUS", "Invalid device configuration: %u",(unsigned)index);
            modbus_config_f = false;
            return;
        }
        for (uint8_t j = 0; j < modbus_config[index].register_cnt;j++)
        {
            if (modbus_config[index].Register_Name[j] == NULL ||
                Modbuscommand_array[index][j] == NULL ||
                payload[index].register_Name[j] == NULL ||
                payload[index].result[j] == NULL)
            {
                ESP_LOGE("MODBUS","NULL register pointer: device=%u, register=%u",(unsigned)index, (unsigned)j);
                modbus_config_f = false;
                return;
            }

            uint8_t expected_byte_cnt = 0;
            uint8_t actual_byte_cnt = 0;

            uart_flush(UART_NUM_1);
            int bytes_sent = uart_write_bytes( UART_NUM_1,(const char *)Modbuscommand_array[index][j],8);

            if (bytes_sent != 8)
            {
                ESP_LOGE("MODBUS", "Device %u: command send failed (%d bytes)",  (unsigned)index, bytes_sent);
                continue;
            }

            memset(modbus_response, 0, sizeof(modbus_response));

            int len = uart_read_bytes( UART_NUM_1, modbus_response,  sizeof(modbus_response), pdMS_TO_TICKS(5000));


                        /* Copy device information */
                        payload[index].device_no = modbus_config[index].device_no;

                        snprintf(payload[index].device_name,sizeof(payload[index].device_name), "%s",modbus_config[index].Device_Name );

                        /* Copy register name into the payload */
                        if (modbus_config[index].Register_Name != NULL &&
                            modbus_config[index].Register_Name[j] != NULL &&
                            payload[index].register_Name != NULL &&
                            payload[index].register_Name[j] != NULL)
                        {
                            snprintf(
                                payload[index].register_Name[j],
                                64,
                                "%s",
                                modbus_config[index].Register_Name[j]
                            );

                            ESP_LOGI("MODBUS", "Register name: %s",
                                    payload[index].register_Name[j]);
                        }
                        else
                        {
                            ESP_LOGE("MODBUS",
                                    "Invalid register name pointer: device=%u, register=%u",
                                    (unsigned)index, (unsigned)j);
                        }

            if(len > 0)
            {
                /* Slave ID must match. A function code with bit 7 set is an exception. */
                if (modbus_response[0] != Modbuscommand_array[index][j][0]) // check slave ID is matched or not 
                {
                    ESP_LOGE("MODBUS", "Device %u: slave ID mismatch", (unsigned)index);
                    continue;
                }
                if (modbus_response[1] != Modbuscommand_array[index][j][1]) // check the function code is present or not
                {
                    ESP_LOGE("MODBUS", "Device %u: function code mismatch", (unsigned)index);
                    continue;
                }


                actual_byte_cnt = modbus_response[2];
                if((modbus_config[index].Quantity[j]) == 0)
                {
                    expected_byte_cnt = 1;
                }
                else
                {
                    expected_byte_cnt = ((uint8_t)(modbus_config[index].Quantity[j]) * 2U);
                }

                if(actual_byte_cnt != expected_byte_cnt)
                {
                    ESP_LOGE("MODBUS", "Device %u: invalid response byte count/length", (unsigned)index);
                    continue;                    
                }
                switch(actual_byte_cnt)
                {

                    case 1:  
                    {                 
                        uint8_t raw = modbus_response[3];
                        double decimal_value = pow(10, modbus_config[index].decimal[j]);
                        if (decimal_value == 0)
                        {
                            payload[index].value[j] = 0.0;
                            snprintf(payload[index].result[j],10, "%s","FAIL");
                            break;
                        }
                        double value;
                        if (modbus_config[index].data_type[j] == UINT8)
                        {
                            value = (double)raw;
                        }
                        else if (modbus_config[index].data_type[j] == INT8)
                        {
                            value = (raw < 128) ? (double)raw : (double)((int16_t)raw - 256);
                        }
                        else
                        {
                            payload[index].value[j] = 0.0;
                            snprintf(payload[index].result[j],  10,"%s", "FAIL");
                            break;
                        }
                        value = (value / (double)decimal_value) * modbus_config[index].slope[j] +  modbus_config[index].offset[j];
                        payload[index].value[j] = value;
                        snprintf(payload[index].result[j],  10, "%s", "Success");
                        break;
                    }

                        case 2:
                        {
                            uint8_t byte0 = modbus_response[3];
                            uint8_t byte1 = modbus_response[4];
                            // Handle byte order
                            switch (modbus_config[index].byte_order[j])
                            {
                                case BYTE_ORDER_ABCD:
                                case BYTE_ORDER_CDAB:
                                    // Normal order
                                    break;

                                case BYTE_ORDER_BADC:
                                case BYTE_ORDER_DCBA:
                                    uint8_t temp = byte0;
                                    byte0 = byte1;
                                    byte1 = temp;
                                    break;

                                default:
                                    payload[index].value[j] = 0.0;
                                    snprintf(payload[index].result[j], 10,"%s", "FAIL");
                                    break;
                            }

                            if (modbus_config[index].byte_order[j] != BYTE_ORDER_ABCD &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_BADC && 
                                modbus_config[index].byte_order[j] != BYTE_ORDER_CDAB && 
                                modbus_config[index].byte_order[j] != BYTE_ORDER_DCBA)
                            {
                                break;
                            }
                            uint16_t raw = ((uint16_t)byte0 << 8) | byte1;
                            double value;
                            double decimal_value = pow(10.0, modbus_config[index].decimal[j]);
                            if (decimal_value == 0.0)
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j],10,"%s", "FAIL");
                                break;
                            }

                            if (modbus_config[index].data_type[j] == UINT16)
                            {
                                value = (double)raw;
                            }
                            else if (modbus_config[index].data_type[j] == INT16)
                            {
                                int32_t signed_value = (raw <= INT16_MAX)  ? (int32_t)raw : (int32_t)raw - 65536;
                                value = (double)signed_value;
                            }
                            else
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j],10,"%s", "FAIL");
                                break;
                            }

                            value = (value / decimal_value) * modbus_config[index].slope[j] + modbus_config[index].offset[j];
                            payload[index].value[j] = value;
                            snprintf(payload[index].result[j], 10, "%s", "Success");
                            break;

                        }

                        case 4:
                        {
                            uint8_t b0 = modbus_response[3];
                            uint8_t b1 = modbus_response[4];
                            uint8_t b2 = modbus_response[5];
                            uint8_t b3 = modbus_response[6];
                            uint8_t temp;
                            switch (modbus_config[index].byte_order[j])
                            {
                                case BYTE_ORDER_ABCD:
                                    // Original order: A B C D
                                    break;

                                case BYTE_ORDER_BADC:
                                    // Swap bytes within each 16-bit word: B A D C
                                    temp = b0;
                                    b0 = b1;
                                    b1 = temp;

                                    temp = b2;
                                    b2 = b3;
                                    b3 = temp;
                                    break;

                                case BYTE_ORDER_CDAB:
                                    // Swap the two 16-bit words: C D A B
                                    temp = b0;
                                    b0 = b2;
                                    b2 = temp;

                                    temp = b1;
                                    b1 = b3;
                                    b3 = temp;
                                    break;

                                case BYTE_ORDER_DCBA:
                                    // Reverse all four bytes: D C B A
                                    temp = b0;
                                    b0 = b3;
                                    b3 = temp;
                                    temp = b1;
                                    b1 = b2;
                                    b2 = temp;
                                    break;

                                default:
                                    payload[index].value[j] = 0.0;
                                    snprintf(payload[index].result[j],   10,"%s", "FAIL");
                                    break;
                            }
                            // Validate byte-order configuration
                            if (modbus_config[index].byte_order[j] != BYTE_ORDER_ABCD &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_BADC &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_CDAB &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_DCBA)
                            {
                                break;
                            }

                            // Combine the bytes into a 32-bit bit pattern
                            uint32_t raw = ((uint32_t)b0 << 24) |((uint32_t)b1 << 16) | ((uint32_t)b2 << 8)  | ((uint32_t)b3);
                            double value;
                            double decimal_value = pow(10.0, modbus_config[index].decimal[j]);
                            if (decimal_value == 0.0)
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j],  10,"%s", "FAIL");
                                break;
                            }

                            if (modbus_config[index].data_type[j] == UINT32)
                            {
                                value = (double)raw;
                            }
                            else if (modbus_config[index].data_type[j] == INT32)
                            {
                                int64_t signed_value;
                                if (raw <= INT32_MAX)
                                {
                                    signed_value = (int64_t)raw;
                                }
                                else
                                {
                                    signed_value = (int64_t)raw - 4294967296LL;
                                }
                                value = (double)signed_value;
                            }
                            else if (modbus_config[index].data_type[j] == FLOAT32)
                            {
                                float float_value;
                                // Interpret the bit pattern as IEEE-754 float
                                memcpy(&float_value, &raw, sizeof(float_value));
                                value = (double)float_value;
                            }
                            else
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j],  10, "%s", "FAIL");
                                break;
                            }
                            // Apply decimal scaling, slope, and offset
                            value = ((value / decimal_value)  * modbus_config[index].slope[j]+ modbus_config[index].offset[j]);
                            payload[index].value[j] = value;
                            snprintf(payload[index].result[j], 10, "%s", "Success");
                            break;

                        }
                        case 6:
                        {
                            // Read 6 data bytes (3 Modbus registers)
                            uint8_t b[6];
                            for (uint8_t k = 0; k < 6; k++)
                            {
                                b[k] = modbus_response[3 + k];
                            }

                            uint8_t temp;
                            // Apply byte order
                            switch (modbus_config[index].byte_order[j])
                            {
                                case BYTE_ORDER_ABCD:
                                    // Keep original order
                                    break;

                                case BYTE_ORDER_BADC:
                                    // Swap adjacent bytes
                                    for (uint8_t k = 0; k < 6; k += 2)
                                    {
                                        temp = b[k];
                                        b[k] = b[k + 1];
                                        b[k + 1] = temp;
                                    }
                                    break;

                                case BYTE_ORDER_CDAB:
                                    // Swap bytes within each 16-bit word pair
                                    // This convention needs to be defined for 3 registers.
                                    payload[index].value[j] = 0.0;
                                    snprintf(payload[index].result[j], 10, "%s", "FAIL");
                                    break;

                                case BYTE_ORDER_DCBA:
                                    // Reverse all 6 bytes
                                    for (uint8_t k = 0; k < 3; k++)
                                    {
                                        temp = b[k];
                                        b[k] = b[5 - k];
                                        b[5 - k] = temp;
                                    }
                                    break;

                                default:
                                    payload[index].value[j] = 0.0;
                                    snprintf(payload[index].result[j], 10, "%s", "FAIL");
                                    break;
                            }

                            // Validate byte order
                            if (modbus_config[index].byte_order[j] != BYTE_ORDER_ABCD &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_BADC &&
                                modbus_config[index].byte_order[j] != BYTE_ORDER_DCBA)
                            {
                                break;
                            }

                            // Combine 6 bytes into a 48-bit unsigned value
                            uint64_t raw = 0;

                            for (uint8_t k = 0; k < 6; k++)
                            {
                                raw = (raw << 8) | (uint64_t)b[k];
                            }

                            double value;
                            double decimal_value = pow(10.0, modbus_config[index].decimal[j]);

                            if (decimal_value == 0.0)
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j], 10, "%s", "FAIL");
                                break;
                            }

                            if (modbus_config[index].data_type[j] == UINT48)
                            {
                                value = (double)raw;
                            }
                            else if (modbus_config[index].data_type[j] == INT48)
                            {
                                int64_t signed_value;

                                // Sign-extend the 48-bit two's-complement value
                                if (raw & (1ULL << 47))
                                {
                                    signed_value = (int64_t)raw - (1LL << 48);
                                }
                                else
                                {
                                    signed_value = (int64_t)raw;
                                }

                                value = (double)signed_value;
                            }
                            else
                            {
                                payload[index].value[j] = 0.0;
                                snprintf(payload[index].result[j], 10, "%s", "FAIL");
                                break;
                            }

                            value = ((value / decimal_value) * modbus_config[index].slope[j]) +modbus_config[index].offset[j];
                            payload[index].value[j] = value;
                            snprintf(payload[index].result[j], 10, "%s", "Success");
                            break;
                        }

                            case 8:
                        { 
                            uint8_t b[8];
                                // Read 8 data bytes from the Modbus response
                                for (uint8_t k = 0; k < 8; k++)
                                {
                                    b[k] = modbus_response[3 + k];
                                }
                                uint8_t temp;
                                switch (modbus_config[index].byte_order[j])
                                {
                                    case BYTE_ORDER_ABCD:
                                        // Original order
                                        break;

                                    case BYTE_ORDER_BADC:
                                        // Swap adjacent bytes
                                        for (uint8_t k = 0; k < 8; k += 2)
                                        {
                                            temp = b[k];
                                            b[k] = b[k + 1];
                                            b[k + 1] = temp;
                                        }
                                        break;

                                    case BYTE_ORDER_CDAB:
                                        // Swap 16-bit words within each 32-bit half
                                        for (uint8_t k = 0; k < 8; k += 4)
                                        {
                                            temp = b[k];
                                            b[k] = b[k + 2];
                                            b[k + 2] = temp;

                                            temp = b[k + 1];
                                            b[k + 1] = b[k + 3];
                                            b[k + 3] = temp;
                                        }
                                        break;

                                    case BYTE_ORDER_DCBA:
                                        // Reverse all 8 bytes
                                        for (uint8_t k = 0; k < 4; k++)
                                        {
                                            temp = b[k];
                                            b[k] = b[7 - k];
                                            b[7 - k] = temp;
                                        }
                                        break;

                                    default:
                                        payload[index].value[j] = 0.0;
                                        snprintf(payload[index].result[j],  10, "%s", "FAIL");
                                        break;
                                }

                                // Validate byte order
                                if (modbus_config[index].byte_order[j] != BYTE_ORDER_ABCD &&
                                    modbus_config[index].byte_order[j] != BYTE_ORDER_BADC &&
                                    modbus_config[index].byte_order[j] != BYTE_ORDER_CDAB &&
                                    modbus_config[index].byte_order[j] != BYTE_ORDER_DCBA)
                                {
                                    break;
                                }

                                // Construct the 64-bit bit pattern
                                uint64_t raw = 0;
                                for (uint8_t k = 0; k < 8; k++)
                                {
                                    raw = (raw << 8) | (uint64_t)b[k];
                                }

                                double value;
                                double decimal_value =pow(10.0, modbus_config[index].decimal[j]);
                                if (decimal_value == 0.0)
                                {
                                    payload[index].value[j] = 0.0;

                                    snprintf(payload[index].result[j],10,"%s", "FAIL");
                                    break;
                                }

                                if (modbus_config[index].data_type[j] == UINT64)
                                {
                                    // UINT64 can lose precision when converted to double
                                    value = (double)raw;
                                }
                                else if (modbus_config[index].data_type[j] == INT64)
                                {
                                    int64_t signed_value;
                                    if (raw <= INT64_MAX)
                                    {
                                        signed_value = (int64_t)raw;
                                    }
                                    else
                                    {
                                        // Convert the two's-complement bit pattern safely
                                        signed_value = -1 - (int64_t)(UINT64_MAX - raw);
                                    }

                                    value = (double)signed_value;
                                }
                                else if (modbus_config[index].data_type[j] == FLOAT64)
                                {
                                    double float_value;
                                    // Interpret the bit pattern as IEEE-754 binary64
                                    memcpy(&float_value, &raw, sizeof(float_value));

                                    value = float_value;
                                }
                                else
                                {
                                    payload[index].value[j] = 0.0;
                                    snprintf(payload[index].result[j],   10,"%s", "FAIL");
                                    break;
                                }

                                // Apply decimal scaling, slope, and offset
                                value = ((value / decimal_value) * modbus_config[index].slope[j] + modbus_config[index].offset[j]);
                                payload[index].value[j] = value;
                                snprintf(payload[index].result[j], 10,"%s", "Success");
                                break;

                         }       
                    default:
                            payload[index].value[j] = 0.0;
                            snprintf(payload[index].result[j],   10,"%s", "FAIL");                    
                    break;
                }
            }
            else
            {
                ESP_LOGE("MODBUS","No response: device=%u, register=%u",(unsigned)index, (unsigned)j);
                payload[index].value[j] = 0.0;
                snprintf(payload[index].result[j], 10,"%s", "FAIL");                 
            }
        }

    }
create_mqtt_payload();
}

static void create_mqtt_payload(void)
{
    size_t len = 0;
    size_t remaining = 0;
    int written;

    PAYLOAD_array[0] = '\0';

    /* Create JSON header */
    written = snprintf(PAYLOAD_array, sizeof(PAYLOAD_array), "{\"Device_count\":%u,\"Devices\":[", (unsigned int)modbus_device_cnt);

    if (written < 0 || (size_t)written >= sizeof(PAYLOAD_array))
    {
        PAYLOAD_array[0] = '\0';
        return;
    }

    len = (size_t)written;
    /* Process each device */
    for (uint8_t index = 0; index < modbus_device_cnt; index++)
    {
        /* Separate devices with a comma */
        if (index > 0)
        {
            remaining = sizeof(PAYLOAD_array) - len;
            written = snprintf(PAYLOAD_array + len, remaining, ",");

            if (written < 0 || (size_t)written >= remaining)
            {
                PAYLOAD_array[0] = '\0';
                return;
            }

            len += (size_t)written;
        }

        /* Device number and name */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining,"{\"Device_no\":%d,\"Device_Name\":\"%s\",", payload[index].device_no,payload[index].device_name);

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;

        /* Register names */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining, "\"Register_Name\":[");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;

        for (uint8_t j = 0; j < modbus_config[index].register_cnt; j++)
        {
            if (j > 0)
            {
                remaining = sizeof(PAYLOAD_array) - len;
                written = snprintf(PAYLOAD_array + len, remaining, ",");

                if (written < 0 || (size_t)written >= remaining)
                {
                    PAYLOAD_array[0] = '\0';
                    return;
                }

                len += (size_t)written;
            }

            remaining = sizeof(PAYLOAD_array) - len;
            written = snprintf(PAYLOAD_array + len, remaining,
                               "\"%s\"", payload[index].register_Name[j]);

            if (written < 0 || (size_t)written >= remaining)
            {
                PAYLOAD_array[0] = '\0';
                return;
            }

            len += (size_t)written;
        }

        /* Close Register_Name array */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining, "],");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;

        /* Register status */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining,
                           "\"Status\":[");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;

        for (uint8_t j = 0; j < modbus_config[index].register_cnt; j++)
        {
            if (j > 0)
            {
                remaining = sizeof(PAYLOAD_array) - len;
                written = snprintf(PAYLOAD_array + len, remaining, ",");

                if (written < 0 || (size_t)written >= remaining)
                {
                    PAYLOAD_array[0] = '\0';
                    return;
                }

                len += (size_t)written;
            }

            remaining = sizeof(PAYLOAD_array) - len;
            written = snprintf(PAYLOAD_array + len, remaining, "\"%s\"", payload[index].result[j]);
            if (written < 0 || (size_t)written >= remaining)
            {
                PAYLOAD_array[0] = '\0';
                return;
            }

            len += (size_t)written;
        }

        /* Close Status array */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining, "],");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }
        len += (size_t)written;
        /* Register values */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining,  "\"Value\":[");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;

        for (uint8_t j = 0; j < modbus_config[index].register_cnt; j++)
        {
            if (j > 0)
            {
                remaining = sizeof(PAYLOAD_array) - len;
                written = snprintf(PAYLOAD_array + len, remaining, ",");

                if (written < 0 || (size_t)written >= remaining)
                {
                    PAYLOAD_array[0] = '\0';
                    return;
                }

                len += (size_t)written;
            }

            remaining = sizeof(PAYLOAD_array) - len;
            written = snprintf(PAYLOAD_array + len, remaining,
                               "%.6g", payload[index].value[j]);

            if (written < 0 || (size_t)written >= remaining)
            {
                PAYLOAD_array[0] = '\0';
                return;
            }

            len += (size_t)written;
        }

        /* Close Value array and current device object */
        remaining = sizeof(PAYLOAD_array) - len;
        written = snprintf(PAYLOAD_array + len, remaining, "]}");

        if (written < 0 || (size_t)written >= remaining)
        {
            PAYLOAD_array[0] = '\0';
            return;
        }

        len += (size_t)written;
    }

    /* Close Devices array and root JSON object */
    remaining = sizeof(PAYLOAD_array) - len;
    written = snprintf(PAYLOAD_array + len, remaining, "]}");

    if (written < 0 || (size_t)written >= remaining)
    {
        PAYLOAD_array[0] = '\0';
        return;
    }
    len += (size_t)written;

    ESP_LOGI("MQTT", "Payload: %s", PAYLOAD_array);
}

static void publish_mqtt_payload(esp_mqtt_client_handle_t mqtt_client)
{
    ESP_LOGI("MQTT", "Entered publish_mqtt_payload()");

    if (mqtt_client == NULL)
    {
        ESP_LOGE("MQTT", "MQTT client is NULL");
        return;
    }

    ESP_LOGI("MQTT", "Checking payload");

    if (PAYLOAD_array[0] == '\0')
    {
        ESP_LOGE("MQTT", "Payload is empty");
        return;
    }

    ESP_LOGI("MQTT", "Payload length: %d",
             (int)strlen(PAYLOAD_array));

    ESP_LOGI("MQTT", "Before esp_mqtt_client_publish()");

    int msg_id = esp_mqtt_client_publish(
        mqtt_client,
        PUBLISH_TOPIC,
        PAYLOAD_array,
        0,
        1,
        0
    );

    ESP_LOGI("MQTT", "After esp_mqtt_client_publish()");

    if (msg_id >= 0)
    {
        ESP_LOGI("MQTT", "Payload queued successfully, msg_id=%d", msg_id);
    }
    else
    {
        ESP_LOGE("MQTT", "Failed to queue MQTT payload, msg_id=%d", msg_id);
    }
}

esp_err_t save_wifi_credentials(const char *ssid, const char *password)
{
    if (ssid == NULL || password == NULL ||
        ssid[0] == '\0')
    {
        return ESP_ERR_INVALID_ARG;
    }

    nvs_handle_t handle;
    esp_err_t err;

    err = nvs_open(WIFI_NVS_NAMESPACE, NVS_READWRITE, &handle);

    if (err != ESP_OK)
    {
        ESP_LOGE("NVS", "NVS open failed: %s",
                 esp_err_to_name(err));
        return err;
    }

    // Mark credentials as not ready before updating them.
    err = nvs_set_u8(handle, WIFI_SAVED_FLAG_KEY, 0);

    if (err == ESP_OK)
        err = nvs_commit(handle);

    if (err == ESP_OK)
        err = nvs_set_str(handle, WIFI_SSID_KEY, ssid);

    if (err == ESP_OK)
        err = nvs_set_str(handle, WIFI_PASSWORD_KEY, password);

    if (err == ESP_OK)
        err = nvs_commit(handle);

    // Set the flag only after both strings are saved.
    if (err == ESP_OK)
        err = nvs_set_u8(handle, WIFI_SAVED_FLAG_KEY, 1);

    if (err == ESP_OK)
        err = nvs_commit(handle);

    nvs_close(handle);

    if (err == ESP_OK)
    {
        ESP_LOGI("NVS", "Wi-Fi credentials saved successfully");
    }
    else
    {
        ESP_LOGE("NVS", "Failed to save Wi-Fi credentials: %s",
                 esp_err_to_name(err));
    }

    return err;
}
esp_err_t load_wifi_credentials(char *ssid, size_t ssid_size,char *password,size_t password_size,bool *credentials_saved)
{
    if (ssid == NULL || password == NULL ||
        credentials_saved == NULL ||
        ssid_size == 0 || password_size == 0)
    {
        return ESP_ERR_INVALID_ARG;
    }

    *credentials_saved = false;
    ssid[0] = '\0';
    password[0] = '\0';

    nvs_handle_t handle;
    esp_err_t err;

    err = nvs_open(WIFI_NVS_NAMESPACE, NVS_READONLY, &handle);

    if (err == ESP_ERR_NVS_NOT_FOUND)
    {
        // Namespace has not been created yet.
        return ESP_OK;
    }

    if (err != ESP_OK)
        return err;

    uint8_t saved_flag = 0;

    err = nvs_get_u8(handle, WIFI_SAVED_FLAG_KEY, &saved_flag);

    if (err == ESP_ERR_NVS_NOT_FOUND)
    {
        // No flag means credentials are not configured.
        nvs_close(handle);
        return ESP_OK;
    }

    if (err != ESP_OK)
    {
        nvs_close(handle);
        return err;
    }

    if (saved_flag != 1)
    {
        nvs_close(handle);
        return ESP_OK;
    }

    size_t required_ssid_size = ssid_size;
    size_t required_password_size = password_size;

    err = nvs_get_str(handle, WIFI_SSID_KEY,
                      ssid, &required_ssid_size);

    if (err == ESP_OK)
    {
        err = nvs_get_str(handle, WIFI_PASSWORD_KEY,
                          password, &required_password_size);
    }

    nvs_close(handle);

    if (err != ESP_OK)
    {
        ssid[0] = '\0';
        password[0] = '\0';
        return err;
    }

    *credentials_saved = true;

    ESP_LOGI("NVS", "Wi-Fi credentials loaded successfully");

    return ESP_OK;
}
esp_err_t clear_wifi_credentials(void)
{
    nvs_handle_t handle;
    esp_err_t err;

    err = nvs_open(WIFI_NVS_NAMESPACE, NVS_READWRITE, &handle);

    if (err != ESP_OK)
        return err;

    // Clear the flag first so credentials are not treated as valid.
    err = nvs_set_u8(handle, WIFI_SAVED_FLAG_KEY, 0);

    if (err == ESP_OK)
        err = nvs_commit(handle);

    if (err == ESP_OK)
        err = nvs_erase_key(handle, WIFI_SSID_KEY);

    if (err == ESP_OK || err == ESP_ERR_NVS_NOT_FOUND)
    {
        err = nvs_erase_key(handle, WIFI_PASSWORD_KEY);

        if (err == ESP_ERR_NVS_NOT_FOUND)
            err = ESP_OK;
    }

    if (err == ESP_OK)
        err = nvs_commit(handle);

    nvs_close(handle);

    if (err == ESP_OK)
        ESP_LOGI("NVS", "Wi-Fi credentials cleared");

    return err;
}
void free_modbus_config(MODBUS_CONFIG *config, size_t device_count)
{
    if (config == NULL)
    {
        return;
    }

    for (size_t i = 0; i < device_count; i++)
    {
        // Free register names only when the pointer array exists
        if (config[i].Register_Name != NULL)
        {
            for (size_t j = 0; j < config[i].register_cnt; j++)
            {
                if (config[i].Register_Name[j] != NULL)
                {
                    free(config[i].Register_Name[j]);
                    config[i].Register_Name[j] = NULL;
                }
            }

            free(config[i].Register_Name);
            config[i].Register_Name = NULL;
        }

        // Free only non-NULL register arrays
        if (config[i].Register_address != NULL)
        {
            free(config[i].Register_address);
            config[i].Register_address = NULL;
        }

        if (config[i].Quantity != NULL)
        {
            free(config[i].Quantity);
            config[i].Quantity = NULL;
        }

        if (config[i].data_type != NULL)
        {
            free(config[i].data_type);
            config[i].data_type = NULL;
        }

        if (config[i].byte_order != NULL)
        {
            free(config[i].byte_order);
            config[i].byte_order = NULL;
        }

        if (config[i].offset != NULL)
        {
            free(config[i].offset);
            config[i].offset = NULL;
        }

        if (config[i].slope != NULL)
        {
            free(config[i].slope);
            config[i].slope = NULL;
        }

        if (config[i].decimal != NULL)
        {
            free(config[i].decimal);
            config[i].decimal = NULL;
        }
    }

    // Free the main configuration array
    free(config);
}
void free_payload(PAYLOAD *payload_ptr,  const MODBUS_CONFIG *config,size_t device_count)
{
    if (payload_ptr == NULL)
    {
        return;
    }

    for (size_t i = 0; i < device_count; i++)
    {
        size_t register_count = 0;

        if (config != NULL)
        {
            register_count = config[i].register_cnt;
        }

        // Free individual register names
        if (payload_ptr[i].register_Name != NULL)
        {
            for (size_t j = 0; j < register_count; j++)
            {
                free(payload_ptr[i].register_Name[j]);
            }

            free(payload_ptr[i].register_Name);
            payload_ptr[i].register_Name = NULL;
        }

        // Free individual result strings
        if (payload_ptr[i].result != NULL)
        {
            for (size_t j = 0; j < register_count; j++)
            {
                free(payload_ptr[i].result[j]);
            }

            free(payload_ptr[i].result);
            payload_ptr[i].result = NULL;
        }

        // Free the numeric value array
        free(payload_ptr[i].value);
        payload_ptr[i].value = NULL;
    }

    // Free the outer payload array
    free(payload_ptr);
}