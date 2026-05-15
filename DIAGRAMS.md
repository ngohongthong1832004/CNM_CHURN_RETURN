# Biểu đồ UML - Hệ thống MLOps Dự đoán Khách hàng Rời bỏ (Customer Churn Prediction)

> **Hướng dẫn xuất ảnh:** Copy từng khối code Mermaid vào **https://mermaid.live** → Export PNG/SVG để chèn vào báo cáo.

---

## 1.3 Biểu đồ Trường hợp Sử dụng (Use Case Diagram)

```mermaid
flowchart TB
    subgraph SYS["Hệ thống MLOps Customer Churn Prediction"]
        direction TB
        subgraph DATA["Pipeline Dữ liệu"]
            UC1("Mô phỏng dữ liệu\nkhách hàng")
            UC2("Ingest dữ liệu\nvào Kafka / Data Lake")
            UC3("ETL: Bronze → Silver → Gold")
            UC4("Materialize Feature Store\n(Feast → Redis)")
        end
        subgraph MODEL["Pipeline Mô hình"]
            UC5("Huấn luyện\nmô hình")
            UC6("Đánh giá\nmô hình")
            UC7("Đăng ký Champion\nModel lên Registry")
        end
        subgraph SERVE["Serving & Giám sát"]
            UC8("Dự đoán churn\nđơn lẻ")
            UC9("Dự đoán theo\nCustomer ID")
            UC10("Dự đoán\nbatch hàng loạt")
            UC11("Giám sát\nData Drift")
            UC12("Xem Dashboard\nMetrics")
        end
    end

    DE["\nData Engineer"]
    MLE["\nML Engineer /\nData Scientist"]
    BU["\nBusiness User\n(CSKH)"]
    SCH["\nAirflow Scheduler\n(Tự động)"]

    DE -. "thủ công" .-> UC1
    DE -. "cấu hình" .-> UC3
    SCH -- "23:45 UTC\nhằng ngày" --> UC1
    SCH -- "23:45 UTC" --> UC2
    SCH -- "00:00 UTC" --> UC3
    SCH -- "00:30 UTC" --> UC4
    SCH -- "Chủ nhật\n00:00 UTC" --> UC5
    SCH -- "Chủ nhật" --> UC7

    MLE -. "thủ công" .-> UC5
    MLE -. "thủ công" .-> UC6
    MLE -. "thủ công" .-> UC7
    MLE -. "theo dõi" .-> UC11
    MLE -. "theo dõi" .-> UC12

    BU -- "Gradio UI /\nREST API" --> UC8
    BU -- "REST API" --> UC9
    BU -- "REST API" --> UC10
    BU -. "xem" .-> UC12

    UC5 -- "«include»" --> UC6
    UC6 -- "«include»" --> UC7

    style SYS fill:#e8f4fd,stroke:#2196f3,stroke-width:2px
    style DATA fill:#fff3e0,stroke:#ff9800
    style MODEL fill:#f3e5f5,stroke:#9c27b0
    style SERVE fill:#e8f5e9,stroke:#4caf50
```

---

## 1.4 Biểu đồ Hoạt động (Activity Diagram)

### 1.4.1 Pipeline Hàng ngày & Tái huấn luyện

```mermaid
flowchart TD
    S([START: 23:45 UTC])

    subgraph DAG1["DAG 1 - data_simulator (23:45 UTC hang ngay)"]
        A1["Data Simulator\nSinh ~5.000 ban ghi khach hang\n(age, tenure, usage, churn...)"]
        A2["Publish to Kafka\nTopic: churn.raw.events"]
        A3{Kafka\nOK?}
        A4["Consumer: Kafka to Bronze\nParse JSON - Iceberg append"]
    end

    subgraph DAG2["DAG 2 - lakehouse_etl (00:00 UTC)"]
        B1["init_namespaces\nTao bronze / silver / gold\nneu chua ton tai"]
        B2["bronze_to_silver\nDeduplication + Validation\n(age 18-100, spend>0, no NaN)"]
        B3{Validation\nPassed?}
        B4["Log & Skip\nBan ghi khong hop le"]
        B5["silver_to_gold\nFeature Engineering\n(tenure_age_ratio, spend_per_usage...)"]
        B6["Export Parquet\nGold table to file system"]
        TRIG[["Trigger DAG 3"]]
    end

    subgraph DAG3["DAG 3 - churn_feature_pipeline (00:30 UTC)"]
        C1["feast apply\nSync Feast Registry"]
        C2["feast materialize-incremental\nLoad features vao Redis Online Store"]
    end

    subgraph DAG4["DAG 4 - churn_retraining_pipeline (Chu nhat 00:00 UTC)"]
        D1["Huan luyen song song 6 mo hinh"]
        subgraph PARALLEL["Parallel Tasks"]
            direction LR
            P1["Logistic\nRegression"]
            P2["Decision\nTree"]
            P3["Random\nForest"]
            P4["XGBoost"]
            P5["LightGBM"]
            P6["CatBoost"]
        end
        D2["find_best_model\nQuery MLflow: F1 cao nhat\ntrong 3 gio qua"]
        D3["evaluate_best_model\nAccuracy >= 0.8, F1 >= 0.8"]
        D4{Thresholds\nDat?}
        D5["register_champion\nMLflow Registry\nalias = champion"]
        D6["Alert: Giu model cu\nRetraining Failed"]
    end

    END([END])

    S --> A1 --> A2 --> A3
    A3 -->|Co| A4
    A3 -->|Khong| A2
    A4 --> B1 --> B2 --> B3
    B3 -->|Hop le| B5
    B3 -->|Loi| B4 --> B5
    B5 --> B6 --> TRIG --> C1 --> C2
    C2 --> D1
    D1 --> PARALLEL
    P1 & P2 & P3 & P4 & P5 & P6 --> D2
    D2 --> D3 --> D4
    D4 -->|Dat| D5 --> END
    D4 -->|Khong dat| D6 --> END
```

### 1.4.2 Luồng Xử lý Yêu cầu Dự đoán

```mermaid
flowchart TD
    S([Người dùng gửi request])
    T1{Loại\nyêu cầu?}

    subgraph PATH1["Đường 1: POST /predict/ (trực tiếp)"]
        P1A["Nhận ChurnInput payload\n(Age, Tenure, Usage...)"]
        P1B["Pydantic Validate\nkiểm tra kiểu dữ liệu"]
        P1C{Validate\nOK?}
        P1D["Preprocess: Encode\ncategorical features"]
    end

    subgraph PATH2["Đường 2: POST /predict/by-customer-id"]
        P2A["Nhận customer_id"]
        P2B["Feast: get_online_features\ntừ Redis Online Store"]
        P2C{Feature\nFound?}
        P2D["Lấy feature vector\n(demographics + behavior)"]
        P2E["404: Customer\nNot Found"]
    end

    subgraph COMMON["Xử lý chung"]
        C1["Load MLflow model\nmodels:/customer_churn_model@champion"]
        C2["model.predict(features)"]
        C3["Log prediction\nvào production dataset"]
        C4["Return {'churn': 0|1}"]
    end

    END([Trả kết quả cho người dùng])

    S --> T1
    T1 -->|"Payload trực tiếp"| P1A --> P1B --> P1C
    P1C -->|OK| P1D --> C1
    P1C -->|Lỗi| P1D
    T1 -->|"Customer ID"| P2A --> P2B --> P2C
    P2C -->|Tìm thấy| P2D --> C1
    P2C -->|Không tìm thấy| P2E --> END
    C1 --> C2 --> C3 --> C4 --> END

    style PATH1 fill:#e3f2fd,stroke:#1565c0
    style PATH2 fill:#f9fbe7,stroke:#827717
    style COMMON fill:#fce4ec,stroke:#c62828
```

---

## 1.5 Biểu đồ Trình tự (Sequence Diagram)

```mermaid
sequenceDiagram
    actor User as Người dùng
    participant UI as Gradio UI<br/>:7860
    participant API as FastAPI<br/>:8000
    participant Registry as MLflow Registry<br/>:5000 / MinIO :9000
    participant Redis as Feast Online Store<br/>Redis :6379
    participant Monitor as Monitoring Service<br/>(Evidently + Prometheus)

    rect rgb(227, 242, 253)
        Note over User,Monitor: Kịch bản 1 — Dự đoán với dữ liệu đầu vào trực tiếp
        User->>UI: Nhập thông tin KH<br/>(Age=30, Tenure=39, Usage=14...)
        UI->>API: POST /predict/<br/>{ ChurnInput JSON }
        API->>API: Pydantic validate input
        API->>Registry: Load model@champion<br/>(lần đầu — sau đó cache lại)
        Registry-->>API: CatBoost / XGBoost model object
        API->>API: Encode categorical features<br/>(subscription_type, contract_length, gender)
        API->>API: model.predict(X)
        API->>Monitor: Log (input_features, prediction=0)<br/>→ production_data.csv
        Monitor-->>API: OK
        API-->>UI: { "churn": 0 }
        UI-->>User: Khách hàng KHÔNG rời bỏ
    end

    rect rgb(243, 229, 245)
        Note over User,Monitor: Kịch bản 2 — Dự đoán theo Customer ID (Feast Online Serving)
        User->>API: POST /predict/by-customer-id<br/>{ "customer_id": "1776312653" }
        API->>Redis: get_online_features(<br/>  entity_rows=[{customer_id: ...}],<br/>  features=[demographics, behavior]<br/>)
        alt Tìm thấy customer
            Redis-->>API: Feature vector<br/>(age=45, tenure=12, support_calls=8...)
            API->>Registry: Load model@champion (cached)
            Registry-->>API: Model
            API->>API: model.predict(feast_features)
            API->>Monitor: Log prediction
            API-->>User: { "churn": 1 }
            Note right of User: Khách hàng CÓ nguy cơ rời bỏ
        else Không tìm thấy
            Redis-->>API: KeyError / Empty
            API-->>User: 404 Customer not found
        end
    end

    rect rgb(232, 245, 233)
        Note over User,Monitor: Kịch bản 3 — Dự đoán Batch
        User->>API: POST /predict/batch<br/>[ {ChurnInput}, {ChurnInput}, ... ]
        loop Mỗi record (tối đa 1000)
            API->>API: Validate + preprocess
            API->>API: model.predict(X_i)
            API->>Monitor: Log prediction_i
        end
        API-->>User: [ {"churn":1}, {"churn":0}, ... ]
    end

    rect rgb(255, 243, 224)
        Note over User,Monitor: Kịch bản 4 — Kiểm tra Data Drift
        User->>API: GET /monitor/drift
        API->>Monitor: Load production_data.csv<br/>(các dự đoán đã log)
        API->>Monitor: Load reference_data.parquet<br/>(training baseline)
        Monitor->>Monitor: Evidently: ColumnDriftReport<br/>(so sánh phân phối các feature)
        Monitor-->>API: drift_score, drifted_features,<br/>performance_degradation
        API-->>User: JSON drift report
    end
```

---

## 1.6 Biểu đồ Lớp (Class Diagram)

```mermaid
classDiagram
    direction TB

    class GenericBinaryClassifierTrainer {
        -model_type: str
        -model_params: dict
        -random_state: int
        -test_size: float
        -mlflow_config: dict
        +__init__(config_path: str)
        +load_data(parquet_path: str) DataFrame
        +preprocess(df: DataFrame) Tuple
        +train(X_train, y_train) BinaryClassifierWrapper
        +run() void
    }

    class BinaryClassifierWrapper {
        -model: BaseEstimator
        -model_type: str
        -feature_names: List~str~
        +predict(X: DataFrame) ndarray
        +predict_proba(X: DataFrame) ndarray
        +get_feature_names() List~str~
    }

    class ExperimentTracker {
        -tracking_uri: str
        -experiment_name: str
        -artifact_location: str
        -tags: dict
        +__init__(config: dict)
        +start_run(run_name: str) ActiveRun
        +log_params(params: dict) void
        +log_metrics(metrics: dict) void
        +log_model(model, artifact_path: str) void
        +find_best_run(metric: str, hours: int) RunInfo
    }

    class ModelRegistry {
        -tracking_uri: str
        -model_name: str
        +register_model(run_id: str) ModelVersion
        +set_alias(version: str, alias: str) void
        +load_champion_model(alias: str) mlflowModel
        +get_model_info(alias: str) ModelVersion
    }

    class ChurnInput {
        +Age: int
        +Gender: str
        +Tenure: int
        +Usage_Frequency: int
        +Support_Calls: int
        +Payment_Delay: int
        +Subscription_Type: str
        +Contract_Length: str
        +Total_Spend: float
        +Last_Interaction: int
    }

    class ChurnPrediction {
        +churn: int
    }

    class PredictionService {
        -model: mlflowModel
        -feast_store: FeatureStore
        -production_data: List~dict~
        +predict(input: ChurnInput) ChurnPrediction
        +predict_by_id(customer_id: str) ChurnPrediction
        +predict_batch(inputs: List~ChurnInput~) List~ChurnPrediction~
        -_preprocess(input: ChurnInput) DataFrame
        -_log_prediction(features, result: int) void
        -_load_model() void
    }

    class MonitoringService {
        -reference_data_path: str
        -production_log_path: str
        +get_drift_report() dict
        -_load_reference() DataFrame
        -_load_production() DataFrame
        -_run_evidently(ref, prod) Report
    }

    class FeatureStore {
        -config: RepoConfig
        -online_store: RedisOnlineStore
        +apply() void
        +get_online_features(entity_rows, features) FeatureVector
        +materialize_incremental(end_date: datetime) void
    }

    class CustomerEntity {
        +customer_id: str
    }

    class CustomerDemographicsView {
        +age: int
        +gender: str
        +tenure_months: int
        +subscription_type: str
        +contract_length: str
        +event_timestamp: datetime
    }

    class CustomerBehaviorView {
        +usage_frequency: int
        +support_calls: int
        +payment_delay_days: int
        +total_spend: float
        +last_interaction_days: int
        +event_timestamp: datetime
    }

    class CustomerDashboardView {
        +tenure_age_ratio: float
        +spend_per_usage: float
        +support_calls_per_tenure: float
        +spending_group: str
        +tenure_group: str
        +event_timestamp: datetime
    }

    GenericBinaryClassifierTrainer ..> BinaryClassifierWrapper : creates
    GenericBinaryClassifierTrainer --> ExperimentTracker : uses
    ExperimentTracker --> ModelRegistry : registers model
    PredictionService --> ModelRegistry : loads champion
    PredictionService --> FeatureStore : fetches features
    PredictionService ..> ChurnInput : accepts
    PredictionService ..> ChurnPrediction : returns
    MonitoringService ..> PredictionService : reads production log
    FeatureStore --> CustomerEntity : defines join key
    FeatureStore --> CustomerDemographicsView : manages
    FeatureStore --> CustomerBehaviorView : manages
    FeatureStore --> CustomerDashboardView : manages
    CustomerDemographicsView --> CustomerEntity : entity
    CustomerBehaviorView --> CustomerEntity : entity
    CustomerDashboardView --> CustomerEntity : entity
```

---

## 1.7 Biểu đồ Luồng Dữ liệu / Database Diagram

```mermaid
flowchart LR
    subgraph INGEST["Tầng Ingest (Kafka)"]
        K[("Kafka Topic\nchurn.raw.events\nJSON messages")]
    end

    subgraph LAKE["Data Lakehouse (Apache Iceberg + Nessie Catalog)"]
        direction TB
        subgraph BRONZE["Bronze Layer (Raw)"]
            B[("bronze.customer_events\n─────────────────\ncustomer_id: BIGINT PK\nage, gender, tenure\nusage_frequency\nsupport_calls\npayment_delay\nsubscription_type\ncontract_length\ntotal_spend, last_interaction\nchurn: BIGINT (0|1)\ningest_date: DATE\ncreated_at: TIMESTAMP")]
        end
        subgraph SILVER["Silver Layer (Cleaned)"]
            S[("silver.customers\n─────────────────\ncustomer_id: BIGINT PK\n[Same schema as Bronze]\n─────────────────\nDeduplication:\n  Keep latest by created_at\nValidation:\n  age ∈ [18,100]\n  tenure ∈ [0,120]\n  total_spend > 0\n  No NaN in key fields")]
        end
        subgraph GOLD["Gold Layer (Feature Ready)"]
            G[("gold.churn_features\n─────────────────\ncustomer_id: BIGINT PK\n[All Silver columns]\n─────────────────\nDerived features:\n  tenure_age_ratio: FLOAT\n  spend_per_usage: FLOAT\n  support_calls_per_tenure: FLOAT\n  spending_group: STRING\n  tenure_group: STRING\n  event_timestamp: TIMESTAMP")]
        end
    end

    subgraph MLFLOW_DB["MLflow Backend (MySQL)"]
        ME[("mlflow.experiments\n─────────\nexperiment_id PK\nname\nartifact_location\nlifecycle_stage")]
        MR[("mlflow.runs\n─────────\nrun_id PK\nexperiment_id FK\nstatus\nstart_time, end_time")]
        MM[("mlflow.metrics\n─────────\nrun_id FK\nkey: training_f1\nvalue: FLOAT\nstep, timestamp")]
        MP[("mlflow.params\n─────────\nrun_id FK\nkey: C, max_depth...\nvalue: STRING")]
        MV[("mlflow.model_versions\n─────────\nname: customer_churn_model\nversion PK\nrun_id FK\nstatus, aliases\n  (champion)")]
    end

    subgraph FEAST["Feature Store"]
        PARQ[/"Parquet Files\n(Gold export)"/]
        REG[("Feast Registry\nSQLite: registry.db\nEntities, FeatureViews,\nFeatureServices")]
        REDIS[("Redis Online Store\n:6379\n─────────────────\nKey pattern:\ncustomer|{id}|{feature}\n─────────────────\nEx: customer|123|age = 35\n    customer|123|churned = 0")]
    end

    K -- "Kafka Consumer\n(append-only)" --> B
    B -- "Deduplicate +\nValidate" --> S
    S -- "Feature\nEngineering" --> G
    G -- "Export\nParquet" --> PARQ
    PARQ -- "feast apply +\nmaterialize" --> REG
    REG --> REDIS

    ME --> MR
    MR --> MM
    MR --> MP
    MR --> MV

    G -. "Training\nData" .-> MR
    MV -. "champion model\nloaded by API" .-> REDIS

    style INGEST fill:#ffebee,stroke:#e53935
    style LAKE fill:#e3f2fd,stroke:#1565c0
    style BRONZE fill:#fff8e1,stroke:#f9a825
    style SILVER fill:#e8f5e9,stroke:#2e7d32
    style GOLD fill:#fce4ec,stroke:#ad1457
    style MLFLOW_DB fill:#f3e5f5,stroke:#6a1b9a
    style FEAST fill:#e0f7fa,stroke:#00838f
```

---

## 1.8 Biểu đồ Mối quan hệ Dữ liệu (Entity Relationship Diagram)

```mermaid
erDiagram
    CUSTOMER_ENTITY {
        string customer_id PK "join_key cho Feast"
    }

    CUSTOMER_DEMOGRAPHICS {
        string customer_id FK
        int    age              "tuổi (18–100)"
        string gender           "Male / Female"
        int    tenure_months    "thời gian dùng dịch vụ"
        string subscription_type "Basic/Standard/Premium"
        string contract_length  "Monthly/Annual/Quarterly"
        timestamp event_timestamp
    }

    CUSTOMER_BEHAVIOR {
        string customer_id FK
        int   usage_frequency       "số lần sử dụng/tháng"
        int   support_calls         "số cuộc gọi hỗ trợ"
        int   payment_delay_days    "ngày trễ thanh toán"
        float total_spend           "tổng chi tiêu (USD)"
        int   last_interaction_days "ngày kể từ lần cuối tương tác"
        timestamp event_timestamp
    }

    CUSTOMER_DASHBOARD_FEATURES {
        string customer_id FK
        float  tenure_age_ratio            "tenure / age"
        float  spend_per_usage             "total_spend / usage_frequency"
        float  support_calls_per_tenure    "support_calls / tenure_months"
        string spending_group              "Low/Medium/High"
        string tenure_group               "New/Mid/Long"
        timestamp event_timestamp
    }

    CHURN_TARGET {
        string customer_id FK
        int    churned          "0 = không rời, 1 = rời bỏ"
        timestamp event_timestamp
    }

    MLFLOW_EXPERIMENT {
        string experiment_id PK
        string name
        string artifact_location
        string lifecycle_stage
    }

    MLFLOW_RUN {
        string run_id PK
        string experiment_id FK
        string status           "RUNNING/FINISHED/FAILED"
        float  training_f1_score
        float  validation_accuracy
        string model_type       "catboost/xgboost/..."
        timestamp start_time
    }

    MODEL_VERSION {
        string name    PK     "customer_churn_model"
        string version PK
        string run_id  FK
        string aliases        "champion"
        string model_uri      "models:/...@champion"
        timestamp created_at
    }

    PREDICTION_LOG {
        int    id PK
        string customer_id FK
        int    age
        int    tenure
        int    usage_frequency
        int    support_calls
        int    payment_delay
        float  total_spend
        int    churn_prediction  "0 hoặc 1"
        string model_version     FK
        timestamp prediction_time
    }

    CUSTOMER_ENTITY ||--|| CUSTOMER_DEMOGRAPHICS       : "có demographics"
    CUSTOMER_ENTITY ||--|| CUSTOMER_BEHAVIOR           : "có behavior"
    CUSTOMER_ENTITY ||--|| CUSTOMER_DASHBOARD_FEATURES : "có derived features"
    CUSTOMER_ENTITY ||--|| CHURN_TARGET                : "có nhãn churn"
    CUSTOMER_ENTITY ||--o{ PREDICTION_LOG              : "có lịch sử dự đoán"

    MLFLOW_EXPERIMENT ||--o{ MLFLOW_RUN                : "chứa các run"
    MLFLOW_RUN        ||--o| MODEL_VERSION             : "đăng ký model"
    MODEL_VERSION     ||--o{ PREDICTION_LOG            : "tạo ra dự đoán"
```

---

## 1.9 Thiết kế Giao diện (UI Design)

### Giao diện 1 — Gradio UI: Customer Churn Prediction (`:7860`)

```
┌─────────────────────────────────────────────────────────────────────┐
│  🤖  Customer Churn Prediction System                               │
│  Powered by MLflow Champion Model + Feast Feature Store             │
├──────────────────────────────────┬──────────────────────────────────┤
│  📋 THÔNG TIN KHÁCH HÀNG         │  📊 KẾT QUẢ DỰ ĐOÁN             │
│                                  │                                  │
│  Tuổi (Age):  [    30    ]       │  ┌────────────────────────────┐  │
│  Giới tính:   [ Male  ▼  ]       │  │                            │  │
│  Thời gian    [    39    ]       │  │   ✅  KHÔNG RỜI BỎ         │  │
│  dùng dịch vụ (tháng)            │  │     Churn = 0              │  │
│                                  │  │                            │  │
│  Tần suất sử dụng:  [   14  ]   │  │   Xác suất rời bỏ: 23%    │  │
│  Số cuộc gọi hỗ trợ:[    5  ]  │  └────────────────────────────┘  │
│  Ngày trễ thanh toán:[   18  ]  │                                  │
│  Loại gói:  [ Standard   ▼  ]   │  📋 Chi tiết Feature:            │
│  Loại HĐ:   [ Annual     ▼  ]   │  • tenure_age_ratio: 1.30        │
│  Tổng chi tiêu: [   932.0  ]    │  • spend_per_usage:  66.57       │
│  Ngày cuối TT:  [    17    ]    │  • spending_group:   High         │
│                                  │  • tenure_group:    Mid           │
│  ┌─────────────┐  ┌──────────┐  │                                  │
│  │  DỰ ĐOÁN   │  │ XÓA       │  │                                  │
│  │  NGAY       │  │ FORM     │  │                                  │
│  └─────────────┘  └──────────┘  │                                  │
├──────────────────────────────────┴──────────────────────────────────┤
│  ─── HOẶC: DỰ ĐOÁN THEO CUSTOMER ID ───                            │
│  Customer ID:  [  1776312653  ]        [ TÌM KIẾM & DỰ ĐOÁN ]      │
│                                                                     │
│  ─── DỰ ĐOÁN HÀNG LOẠT (Batch) ───                                 │
│  Upload CSV:   [ Chọn file... ]        [ BATCH PREDICT ]            │
└─────────────────────────────────────────────────────────────────────┘
```

### Giao diện 2 — FastAPI Swagger UI: REST API Documentation (`:8000/docs`)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Customer Churn Prediction API                     OpenAPI 3.1      │
│  FastAPI | MLflow Champion Model | Feast Feature Store              │
├─────────────────────────────────────────────────────────────────────┤
│  PREDICT                                                            │
│  ├─ POST  /predict/              Dự đoán với payload đầy đủ         │
│  ├─ POST  /predict/by-customer-id  Dự đoán theo Customer ID         │
│  └─ POST  /predict/batch         Dự đoán hàng loạt (max 1000)      │
│                                                                     │
│  MONITOR                                                            │
│  └─ GET   /monitor/drift         Evidently data drift report        │
│                                                                     │
│  HEALTH                                                             │
│  ├─ GET   /health/               Model status + timestamp           │
│  └─ GET   /metrics               Prometheus metrics endpoint        │
├─────────────────────────────────────────────────────────────────────┤
│  Schemas                                                            │
│  ChurnInput  { Age, Gender, Tenure, Usage_Frequency,                │
│                Support_Calls, Payment_Delay, Subscription_Type,     │
│                Contract_Length, Total_Spend, Last_Interaction }     │
│  ChurnPrediction { churn: integer (0|1) }                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Giao diện 3 — MLflow Experiment Tracking UI (`:5000`)

```
┌─────────────────────────────────────────────────────────────────────┐
│  MLflow Experiments                                   [+ New Exp]   │
├──────────────┬──────────────────────────────────────────────────────┤
│ Experiments  │  Experiment: churn_retraining_pipeline               │
│              │  ─────────────────────────────────────────────       │
│ ▶ churn_     │  Run Name        │ Model     │ F1    │ Accuracy       │
│   retraining │  run_catboost_   │ CatBoost  │ 0.867 │ 0.872  🏆      │
│   _pipeline  │  run_xgboost_    │ XGBoost   │ 0.854 │ 0.861          │
│ ▶ churn_     │  run_lgbm_       │ LightGBM  │ 0.848 │ 0.856          │
│   prediction │  run_rf_         │ RandomF.  │ 0.832 │ 0.841          │
│   _logistic  │  run_dt_         │ Dec.Tree  │ 0.791 │ 0.803          │
│              │  run_lr_         │ LogReg    │ 0.761 │ 0.775          │
│ Models       │                                                       │
│ ▶ customer_  │  Registered Model: customer_churn_model              │
│   churn_     │  Version 7 │ alias: champion │ CatBoost               │
│   model      │  Artifact: s3://mlflow/catboost/model.pkl            │
└──────────────┴──────────────────────────────────────────────────────┘
```

### Giao diện 4 — Airflow DAG UI (`:8080`)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Apache Airflow 3.1.1                    [DAGs] [Grid] [Graph]       │
├─────────────────────────────────────────────────────────────────────┤
│  DAG: churn_retraining_pipeline           Schedule: 0 0 * * 0       │
│                                                                     │
│  ┌──────────────────────────────── Graph View ───────────────────┐  │
│  │                                                               │  │
│  │  [train_lr] ─┐                                               │  │
│  │  [train_dt] ─┤                                               │  │
│  │  [train_rf] ─┼──► [find_best_model] ──► [evaluate] ──► [reg] │  │
│  │  [train_xgb]─┤         ↑                                     │  │
│  │  [train_lgbm]┤    MLflow Query                               │  │
│  │  [train_cb] ─┘    (max F1, 3h)                              │  │
│  │                                                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  DAGs                   Status    Schedule         Last Run         │
│  data_simulator         ✅ OK     0 23 45 * * *    2026-05-07       │
│  lakehouse_etl          ✅ OK     0 0 * * *         2026-05-07       │
│  churn_feature_pipeline ✅ OK     30 0 * * *        2026-05-07       │
│  churn_retraining_pipe  ✅ OK     0 0 * * 0         2026-05-05       │
└─────────────────────────────────────────────────────────────────────┘
```

### Giao diện 5 — Grafana Monitoring Dashboard (`:3000`)

```
┌─────────────────────────────────────────────────────────────────────┐
│  Grafana — Customer Churn MLOps Dashboard          [Last 24h ▼]     │
├──────────────────────┬──────────────────────┬───────────────────────┤
│  📈 Requests / min   │  ⏱️  Latency (p95)    │  ✅ Success Rate       │
│                      │                      │                       │
│   ▁▃▅▇▇▅▃▁▃▅▇▇▅▃▁  │        128 ms        │        99.2 %         │
│        42 req/min    │                      │                       │
├──────────────────────┴──────────────────────┴───────────────────────┤
│  🔴 Churn Predictions Today    │  📊 Prediction Distribution        │
│                                │                                    │
│   Total: 1,247                 │   Churn (1): ████████  62%         │
│   Churn: 773  (62%)            │   No Churn: █████      38%         │
│   No Churn: 474 (38%)          │                                    │
├────────────────────────────────┴────────────────────────────────────┤
│  ⚠️  Data Drift Monitor                                              │
│  Feature Drift Score: 0.031  ✅ Normal (threshold: 0.1)             │
│  Drifted features: None                                             │
│  Last check: 2026-05-07 06:00 UTC                                   │
├─────────────────────────────────────────────────────────────────────┤
│  📋 Airflow DAG Status (via StatsD)                                  │
│  data_simulator:         ✅ Last run: success  Duration: 2m 14s     │
│  lakehouse_etl:          ✅ Last run: success  Duration: 8m 32s     │
│  churn_feature_pipeline: ✅ Last run: success  Duration: 1m 05s     │
└─────────────────────────────────────────────────────────────────────┘
```

---

> **Lưu ý xuất ảnh cho báo cáo:**
> - **Mermaid diagrams (1.3 → 1.8):** Copy từng code block vào https://mermaid.live → Actions → Export PNG
> - **UI Mockups (1.9):** Chụp màn hình thực tế từ hệ thống đang chạy ở các port tương ứng, hoặc dùng ASCII art trên để minh họa
> - **PlantUML thay thế:** Nếu cần style UML chuẩn hơn, dùng https://plantuml.com/plantuml/uml/ với code PlantUML tương đương
