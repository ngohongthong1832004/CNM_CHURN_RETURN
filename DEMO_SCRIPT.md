# 🎬 Demo Script: MLOps End-to-End Customer Churn Prediction System

**Duration:** ~15-20 minutes (5 min intro + 10 min walkthrough + 5 min Q&A)

**Prerequisite:** Hệ thống đã chạy `.\start.ps1` hoàn tất (Phase 1-4), toàn bộ services sẵn sàng.

---

## 📋 Chuẩn bị trước demo (5 phút)

### Setup screen và browser tabs
1. **Open Airflow UI:** http://localhost:8080 (username: `airflow`, password: `airflow`)
   - Tab này để show DAG orchestration
   
2. **Open Trino/Superset:** http://localhost:8088 (username: `admin`, password: `admin`)
   - Để query gold table và vẽ chart
   
3. **Open Gradio UI:** http://localhost:7860
   - Để demo prediction interface
   
4. **Open MLflow Registry:** http://localhost:5000
   - Để show model champion và metrics
   
5. **Open Grafana:** http://localhost:3000 (username: `admin`, password: `admin`)
   - Để show monitoring/metrics
   
6. **Terminal (PowerShell):** Sẵn sàng chạy command
   - Để show logs và query data

### Arrange display
- **Laptop/Desktop screen:** 1920x1080 minimum
- **Zoom level:** 150% hoặc Full HD để text dễ đọc khi ghi hình
- **Background:** Tắt notification, fullscreen browsers
- **Recording:** OBS/ScreenFlow/Camtasia; setup 30fps 1080p trước khi bắt đầu

---

## 🎥 DEMO FLOW (15 phút)

### **SCENE 1: Giới thiệu kiến trúc (2 phút)**

**Nội dung nói:**
> "Đây là một hệ thống MLOps end-to-end dự đoán khách hàng bỏ cuộc (customer churn). 
> Hệ thống này có 4 lớp chính:
> 1. **Data Layer**: Simulator sinh dữ liệu → Kafka → Iceberg lakehouse (Bronze → Silver → Gold)
> 2. **Feature Layer**: Feast feature store + Redis online store
> 3. **Model Layer**: Airflow train 6 models parallel, chọn champion, register vào MLflow
> 4. **Serving Layer**: FastAPI + Gradio UI + Monitoring
>
> Toàn bộ được điều phối bởi Airflow DAGs chạy hàng ngày."

**Hiển thị trên màn hình:**
- Mở README.md, scroll tới architecture diagram
- Hoặc vẽ simple diagram trên slide PowerPoint trước khi bắt đầu (nếu có)
- Keep diagram này visible trong background

---

### **SCENE 2: Data Flow - Từ Simulator tới Gold (3 phút)**

**Thực thi:**

```powershell
# Terminal 1: Xem logs của Kafka topic
docker logs aio_kafka_broker_1 | findstr "churn.raw.events" | tail -20

# Hoặc: Check data trong Bronze table via Trino
```

**Trino SQL (chạy từ Superset hoặc CLI):**
```sql
-- Bronze: raw data từ Kafka
SELECT COUNT(*) as raw_records FROM iceberg.bronze.customer_events;

-- Silver: cleaned, deduplicated
SELECT COUNT(*) as cleaned_records FROM iceberg.silver.customers;

-- Gold: features ready
SELECT COUNT(*) as feature_ready_records FROM iceberg.gold.churn_features;

-- Sample record từ Gold
SELECT customer_id, age, tenure_months, total_spend, churned, spending_group, tenure_group
FROM iceberg.gold.churn_features
LIMIT 5;
```

**Nội dung nói:**
> "Data flow bắt đầu từ simulator sinh ~5000 khách hàng mỗi ngày, gửi vào Kafka topic.
> Airflow DAG 'data_simulator' chạy 23:45 UTC hàng ngày.
> Tiếp đó, DAG 'lakehouse_etl' chạy 00:00:
>   - Consume từ Kafka → Bronze (raw append-only)
>   - Dedup + validate → Silver (cleaned)
>   - Feature engineering → Gold (with derived features like tenure_age_ratio, spending_group, v.v.)
> Gold table sau đó được export sang parquet để Feast dùng."

**Hiển thị trên màn hình:**
- Query kết quả Bronze/Silver/Gold record count
- Show sample Gold row với features
- Highlight cột `spending_group`, `tenure_group`, `tenure_age_ratio`

---

### **SCENE 3: Feature Store & Feast (2 phút)**

**Hiển thị:**
```bash
# Terminal: Xem Feast features
cd data-pipeline/churn_feature_store/churn_features/feature_repo
cat feature_views.py | grep -A 3 "name="
```

**Nội dung nói:**
> "Feast là feature store của chúng ta - nó định nghĩa cách lấy và phục vụ features cho model.
> Chúng tôi có 4 feature views:
>   1. customer_demographics: tuổi, giới tính, tenure, loại subscription
>   2. customer_behavior: tần suất sử dụng, số support calls, payment delay, total spend
>   3. churn_target: nhãn chúng ta dự đoán
>   4. customer_dashboard_features: features cho dashboards (tenure_age_ratio, spend_per_usage, spending_group, tenure_group)
> 
> Features được materialize (cache) vào Redis online store, nên khi serving API chỉ cần query Redis → instant lookup thay vì query database."

**Hiển thị trên màn hình:**
- Cat feature_views.py (hoặc vẽ feature groups trên slide)
- Show Redis connection trong feature_store.yaml
- Optionally: `redis-cli -p 6379 KEYS "*" | head -20` (nếu Redis container có Redis CLI)

---

### **SCENE 4: Model Training (2 phút)**

**Airflow UI:**
- Navigate tới DAG: `churn_retraining_pipeline`
- Scroll down to see the DAG graph

**Nội dung nói:**
> "Model training được trigger bởi DAG 'churn_retraining_pipeline' chạy Chủ nhật 00:00 UTC, 
> hoặc có thể trigger thủ công.
>
> Chúng tôi train 6 models **song song** (parallel):
>   - Logistic Regression
>   - Decision Tree
>   - Random Forest
>   - XGBoost
>   - LightGBM
>   - CatBoost
>
> Sau khi tất cả models hoàn thành, DAG tự động:
> 1. Query MLflow để tìm model có F1-score cao nhất trong 3 giờ qua
> 2. Evaluate threshold validation (F1 ≥ 0.8)
> 3. Register model này vào MLflow Registry với alias 'champion'
> 4. Model champion là phiên bản serving live của chúng ta"

**Hiển thị trên màn hình:**
- Click vào DAG graph → xem visual của parallel training
- Optional: Nếu DAG đã chạy, expand 1 run để show task logs

---

### **SCENE 5: MLflow Model Registry (2 phút)**

**MLflow UI (http://localhost:5000):**

1. **Click "Models" sidebar**
   - Tìm model: `customer_churn_model`

2. **Xem model details:**
   - Version count
   - Aliases (champion)
   - Metrics (F1, accuracy, precision, recall)

3. **Xem 1 model version:**
   - Click vào version (e.g., version 1)
   - Scroll down xem parameters (max_depth, learning_rate, v.v.)
   - Xem metrics logged

**Nội dung nói:**
> "Mỗi model training run được MLflow autolog tất cả:
> - Model hyperparameters (ngành thông số)
> - Training metrics: F1-score, accuracy, precision, recall, AUC
> - Confusion matrix
> - Feature importance (tùy model)
> 
> Phiên bản có F1 cao nhất được tag là 'champion' - đây là model mà API serving sẽ dùng.
> URI của champion model là: models:/customer_churn_model@champion
> Khi serving code gọi URI này, MLflow tự động download artifact + model từ MinIO."

**Hiển thị trên màn hình:**
- Show model list
- Click vào champion version → show metrics
- Highlight F1, accuracy metrics

---

### **SCENE 6: Serving API - Health Check (1 phút)**

**Terminal:**
```bash
# Health check
curl -s http://localhost:8000/health/ | jq '.'
```

**Nội dung nói:**
> "Serving pipeline chạy FastAPI trên port 8000. 
> Health endpoint cho biết:
> - API status (healthy/unhealthy)
> - Model champion URI
> - Feast feature store status
> - Current predictions count"

**Hiển thị trên màn hình:**
- Show curl output với JSON response

---

### **SCENE 7: Serving API - Predict by Customer ID (2 phút)**

**Terminal:**
```bash
# Prediction by customer_id (lookup features từ Redis/Feast)
$customerId = "1776312653"
curl -X POST http://localhost:8000/predict/by-customer-id `
  -H "Content-Type: application/json" `
  -d "{\"customer_id\": \"$customerId\"}" | jq '.'
```

**Nội dung nói:**
> "API có endpoint /predict/by-customer-id.
> Bạn chỉ cần pass customer_id, API sẽ:
> 1. Look up features từ Feast online store (Redis)
> 2. Format features đúng schema của model
> 3. Gọi champion model predict
> 4. Return kết quả: churn = 0 (khách hàng giữ lại) hoặc 1 (dự đoán bỏ cuộc)"

**Hiển thị trên màn hình:**
- Show curl command
- Show JSON response với prediction result

---

### **SCENE 8: Gradio UI - Interactive Demo (2 phút)**

**Browser: http://localhost:7860**

**Nội dung nói:**
> "Gradio UI là giao diện user-friendly cho non-technical users.
> Người dùng có thể:
> 1. Nhập customer_id để auto-lookup và predict
> 2. Nhập manual features để custom predict
> 3. Xem explanation (feature importance)"

**Thực thi:**
1. **Demo 1: By Customer ID**
   - Nhập customer_id: 1776312653
   - Click Predict
   - Show output (churn = 0 hoặc 1, confidence)

2. **Demo 2: Manual Input**
   - Nhập Age: 35
   - Tenure: 24
   - Usage Frequency: 15
   - Support Calls: 2
   - Payment Delay: 5
   - Subscription Type: Premium
   - Contract Length: Annual
   - Total Spend: 500.0
   - Last Interaction: 10
   - Click Predict
   - Show output

**Hiển thị trên màn hình:**
- Record Gradio interface
- Show input fields + output prediction
- Show interpretation/feature importance nếu có

---

### **SCENE 9: Superset Dashboards (2 phút)**

**Browser: http://localhost:8088**
- Login: admin / admin

**Nội dung nói:**
> "Superset là data visualization platform kết nối trực tiếp tới Trino.
> Chúng ta có datasource 'Lakehouse (Trino)' trỏ tới Iceberg tables.
> Các dashboard có thể vẽ từ gold.churn_features:"

**Charts to show:**

1. **Churn Rate by Spending Group:**
   ```sql
   SELECT spending_group, 
          COUNT(*) as total,
          SUM(CAST(churned AS INT)) as churn_count,
          SUM(CAST(churned AS INT)) * 100.0 / COUNT(*) as churn_rate
   FROM iceberg.gold.churn_features
   GROUP BY spending_group
   ORDER BY spending_group;
   ```
   - Create Bar chart: X=spending_group, Y=churn_rate
   - Show: High/Very High spending groups have LOWER churn rate

2. **Average Spend by Tenure Group:**
   ```sql
   SELECT tenure_group, 
          AVG(total_spend) as avg_spend,
          COUNT(*) as customer_count
   FROM iceberg.gold.churn_features
   GROUP BY tenure_group
   ORDER BY tenure_group;
   ```
   - Create Bar chart: X=tenure_group, Y=avg_spend
   - Show: Longer tenure → higher spend

3. **Support Calls vs Churn:**
   ```sql
   SELECT 
          CASE WHEN churned = 0 THEN 'Retained' ELSE 'Churned' END as status,
          AVG(support_calls_per_tenure) as avg_support_calls,
          COUNT(*) as count
   FROM iceberg.gold.churn_features
   GROUP BY churned;
   ```
   - Create comparison table/bar chart
   - Show: Churned customers có support_calls_per_tenure thấp hơn

**Hiển thị trên màn hình:**
- SQL Lab: Write 1-2 queries live
- Show results
- Create 1-2 simple charts
- Highlight insights (e.g., spending group correlation, support calls indicator)

---

### **SCENE 10: Grafana Monitoring (1 phút)**

**Browser: http://localhost:3000**
- Login: admin / admin

**Nội dung nói:**
> "Grafana thu thập metrics từ Prometheus + logs từ Loki.
> Chúng ta có dashboards cho:
> - Infrastructure: CPU, RAM, disk, network (từ node_exporter)
> - FastAPI: request count, latency p50/p95/p99, error rate
> - Airflow: DAG runs, task success/failed, scheduler heartbeat"

**Hiển thị trên màn hình:**
- Click vào 1 dashboard (e.g., "FastAPI Overview")
- Scroll qua metrics
- Show request rate, latency trends
- Optional: Highlight spike nếu vừa chạy prediction

---

### **SCENE 11: Airflow DAG Orchestration (1 phút)**

**Airflow UI (http://localhost:8080):**

**Nội dung nói:**
> "Airflow điều phối toàn bộ pipeline. 
> 4 DAGs chính:
> 1. data_simulator (23:45 UTC): Sinh dữ liệu → Kafka
> 2. lakehouse_etl (00:00 UTC): ETL Bronze→Silver→Gold
> 3. churn_feature_pipeline (00:30 UTC): Feast materialize
> 4. churn_retraining_pipeline (00:00 Chủ nhật): Model training
> 
> Dependency: simulator → lakehouse → feature → (xong → training nếu là Chủ nhật)"

**Hiển thị trên màn hình:**
- Graph view của DAG 4
- Show task dependencies
- Show recent run history (success/failed)

---

### **SCENE 12: Closing - Architecture Recap (1 phút)**

**Nội dung nói (back to architecture diagram):**
> "Tóm lại, hệ thống này demo đầy đủ:
> ✅ **Data Engineering**: Kafka → Iceberg lakehouse (Bronze→Silver→Gold)
> ✅ **Feature Engineering**: Feast feature store + Redis online store
> ✅ **ML Training**: 6 models parallel, automated champion selection
> ✅ **Model Registry**: MLflow centralized model management
> ✅ **Serving**: FastAPI + Gradio UI, low-latency predictions
> ✅ **Monitoring**: Grafana dashboards, drift detection
> ✅ **Orchestration**: Airflow DAGs với schedule + dependencies
> ✅ **Visualization**: Superset BI dashboards từ data lake
>
> Tất cả containerized, reproducible, auto-deploy via Docker Compose + Airflow.
> Bạn có câu hỏi nào không?"

---

## 📹 Recording Tips

### OBS Setup (Free)
```
Resolution: 1920x1080
FPS: 30
Bitrate: 6000 Kbps (high quality)
Encoder: H.264
```

### Key moments to pause & zoom in
- Trino query results (chúng nhỏ, khó đọc trên video)
- MLflow metrics table
- Gradio prediction output
- API curl response

### Audio
- Prepare script trước (tối thiểu bullet points)
- Speak slowly, pause 1-2 sec sau mỗi phần
- Background: Quiet room, USB mic nếu có

### Post-production
- Cut intro 5 sec
- Speed up loading screens (2x), keep interactions real-time
- Add captions nếu có thời gian (YouTube/etc auto-generate)
- Add background music (Lo-fi/ambient, copyright-free)
- Final export: MP4 H.264 1080p30

---

## 🎯 Demo Checklist (bước trước khi bắt đầu quay)

- [ ] Terminal: `docker ps` → confirm all containers running
- [ ] Airflow: Can login, DAGs visible
- [ ] Trino: Can query bronze/silver/gold tables
- [ ] MLflow: Can see champion model in registry
- [ ] Gradio: Load successfully trên http://localhost:7860
- [ ] Superset: Can query gold.churn_features
- [ ] Grafana: Dashboard loading, showing recent metrics
- [ ] Recording software: OBS/Camtasia setup, mic tested
- [ ] Network: Stable (dùng wired nếu possible)
- [ ] Firewall: Không block localhost ports

---

## 🎬 Timeline Summary

| Scene | Title | Duration |
|-------|-------|----------|
| 1 | Architecture Intro | 2 min |
| 2 | Data Flow (Simulator→Gold) | 3 min |
| 3 | Feast Feature Store | 2 min |
| 4 | Model Training | 2 min |
| 5 | MLflow Registry | 2 min |
| 6 | API Health Check | 1 min |
| 7 | API Predict by Customer ID | 2 min |
| 8 | Gradio UI Demo | 2 min |
| 9 | Superset Dashboards | 2 min |
| 10 | Grafana Monitoring | 1 min |
| 11 | Airflow Orchestration | 1 min |
| 12 | Closing Recap | 1 min |
| **Total** | | **21 min** |

---

## 💡 Alternative Shorter Version (10 min)

Nếu muốn demo ngắn hơn, tập trung vào user-facing components:

1. **Intro** (1 min): 1 slide architecture
2. **Data** (1 min): Show Trino gold.churn_features count + sample
3. **Training** (1 min): Show MLflow champion model metrics
4. **Serving** (2 min): API curl + Gradio UI prediction
5. **Dashboard** (3 min): Superset charts
6. **Closing** (1 min): Recap
   
Total: ~10 min, tập trung vào value (data → model → prediction → insights)

---

## 📝 Speaker Notes Outline

```
[SCENE 1]
"We built an end-to-end MLOps system to predict customer churn..."

[SCENE 2]
"Data flows through Kafka into our Iceberg lakehouse..."

[SCENE 3]
"Features are defined in Feast..."

[SCENE 4-5]
"We train 6 models in parallel, pick the best by F1..."

[SCENE 6-8]
"The champion model is served via FastAPI and Gradio..."

[SCENE 9]
"Analysts can create dashboards directly against gold.churn_features in Superset..."

[SCENE 10]
"Everything is monitored in Grafana..."

[SCENE 11]
"All orchestrated by Airflow DAGs running on schedule..."

[SCENE 12]
"This is production-ready, containerized, and reproducible."
```

---

**Good luck with your demo! 🚀**
