from manim import *
import numpy as np

# Configuration
config.pixel_height = 1080
config.pixel_width = 1920
config.frame_rate = 60

CYAN = "#00FFFF"  

class Overview(Scene):
    """Opening scene - System Overview"""
    def construct(self):
        # Title
        title = Text("Anomaly Detection System", font_size=60, weight=BOLD, color=BLUE)
        subtitle = Text("Production-Ready MLOps for Industrial Washing Machines", 
                       font_size=28, color=GRAY, weight=LIGHT)
        subtitle.next_to(title, DOWN, buff=0.3)
        
        self.play(Write(title), run_time=1)
        self.play(Write(subtitle), run_time=1)
        self.wait(1)
        
        # Key characteristics
        char_group = VGroup()
        characteristics = [
            "✓ End-to-end production system",
            "✓ Dual-pipeline feature store",
            "✓ Real-time anomaly detection",
            "✓ Event-driven investigation",
            "✓ Automated retraining"
        ]
        
        for char in characteristics:
            text = Text(char, font_size=24, color=WHITE)
            char_group.add(text)
        
        char_group.arrange(DOWN, aligned_edge=LEFT, buff=0.4)
        char_group.next_to(VGroup(title, subtitle), DOWN, buff=1)
        
        for char_text in char_group:
            self.play(Write(char_text), run_time=0.5)
        
        self.wait(2)
        self.play(FadeOut(VGroup(title, subtitle, char_group)))


class ArchitectureOverview(Scene):
    """Five interconnected pipelines visualization"""
    def construct(self):
        title = Text("Five Interconnected Pipelines", font_size=50, weight=BOLD)
        title.to_edge(UP, buff=0.5)
        self.play(Write(title))
        self.wait(0.5)
        
        # Define pipeline boxes
        pipelines = [
            {
                "name": "DATA PREPARATION",
                "subtitle": "(Run once before online)",
                "color": PURPLE,
                "y": 2.5,
                "flow": "create_datasets → data_eng → batch → training"
            },
            {
                "name": "STREAMING PIPELINE",
                "subtitle": "(Real-time, continuous)",
                "color": BLUE,
                "y": 1,
                "flow": "producer → Redpanda → streaming → Redis"
            },
            {
                "name": "BATCH PIPELINE",
                "subtitle": "(Airflow - daily 00:00 UTC)",
                "color": TEAL,
                "y": -0.5,
                "flow": "batch_service → offline Parquet → materialize → Redis"
            },
            {
                "name": "INFERENCE PIPELINE",
                "subtitle": "(Real-time, continuous)",
                "color": GREEN,
                "y": -2,
                "flow": "Redpanda → Feast → IsolationForest → Redpanda"
            },
            {
                "name": "ANOMALY INVESTIGATION",
                "subtitle": "(Event-driven, on anomaly)",
                "color": ORANGE,
                "y": -3.5,
                "flow": "if_anomaly → LangChain → MCP → vLLM → Slack"
            }
        ]
        
        boxes = VGroup()
        for pipeline in pipelines:
            # Main box
            box = RoundedRectangle(width=10, height=0.8, color=pipeline["color"], 
                                   fill_opacity=0.2, stroke_width=2)
            box.move_to([0, pipeline["y"], 0])
            
            # Labels
            name = Text(pipeline["name"], font_size=20, weight=BOLD, color=pipeline["color"])
            subtitle = Text(pipeline["subtitle"], font_size=14, color=GRAY)
            flow = Text(pipeline["flow"], font_size=12, color=WHITE, weight=LIGHT)
            
            labels = VGroup(name, subtitle, flow).arrange(DOWN, buff=0.1)
            labels.move_to(box)
            
            boxes.add(box, labels)
        
        # Animate pipelines
        for i in range(0, len(boxes), 3):
            self.play(FadeIn(boxes[i:i+3]), run_time=1)
            self.wait(0.5)
        
        self.wait(2)
        self.play(FadeOut(VGroup(title, boxes)))


class DataPreparationFlow(Scene):
    """Detailed Data Preparation Pipeline"""
    def construct(self):
        title = Text("1. Data Preparation Pipeline", font_size=48, weight=BOLD, color=PURPLE)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Service boxes
        services = [
            {
                "name": "create_datasets_service",
                "action": "Generate synthetic sensor data\n3 machines, 1M rows, 2% anomaly rate",
                "x": -5,
                "y": 2
            },
            {
                "name": "data_engineering_service",
                "action": "Compute streaming + batch features\nRolling windows, daily aggregations",
                "x": -5,
                "y": 0.5
            },
            {
                "name": "batch_pipeline_service",
                "action": "Compute Daily_Vibration_PeakMean_Ratio\nCall feast.materialize_incremental()",
                "x": -5,
                "y": -1
            },
            {
                "name": "training_service",
                "action": "Fit IsolationForest Pipeline\nRegister in MLflow (if_anomaly_detector)",
                "x": -5,
                "y": -2.5
            }
        ]
        
        # Draw services and connections
        prev_service = None
        for i, service in enumerate(services):
            box = RoundedRectangle(width=3, height=1.2, color=BLUE, 
                                  fill_opacity=0.3, stroke_width=2)
            box.move_to([service["x"], service["y"], 0])
            
            name = Text(service["name"], font_size=14, weight=BOLD, color=BLUE)
            action = Text(service["action"], font_size=11, color=WHITE)
            
            texts = VGroup(name, action).arrange(DOWN, buff=0.15, aligned_edge=LEFT)
            texts.move_to(box)
            
            self.play(FadeIn(box, texts), run_time=0.6)
            
            # Draw arrow to next service
            if i < len(services) - 1:
                arrow = Arrow(np.array([service["x"] + 1.6, service["y"] - 0.7, 0]),
                            np.array([service["x"] + 1.6, services[i+1]["y"] + 0.7, 0]),
                            buff=0.1, color=WHITE, stroke_width=2)
                self.play(FadeIn(arrow), run_time=0.5)
            
            self.wait(0.3)
        
        # MLflow info
        mlflow_text = Text("Model registered in MLflow", font_size=18, color=GREEN, weight=BOLD)
        mlflow_text.move_to([2, -2.5, 0])
        self.play(FadeIn(mlflow_text), run_time=0.5)
        
        self.wait(1.5)


class StreamingPipelineFlow(Scene):
    """Detailed Streaming Pipeline"""
    def construct(self):
        title = Text("2. Streaming Pipeline", font_size=48, weight=BOLD, color=BLUE)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Create flow diagram
        producer_box = RoundedRectangle(width=2.5, height=0.8, color=BLUE, 
                                       fill_opacity=0.3, stroke_width=2)
        producer_box.move_to([-5, 2, 0])
        producer_text = Text("producer_service\nParquet → Redpanda", font_size=12, color=WHITE)
        producer_text.move_to(producer_box)
        
        self.play(FadeIn(producer_box, producer_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow to Redpanda
        arrow1 = Arrow(producer_box.get_right(), np.array([-2, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow1), run_time=0.4)
        
        redpanda_box = Circle(radius=0.4, color=BLUE, fill_opacity=0.5, stroke_width=2)
        redpanda_box.move_to([-1, 2, 0])
        redpanda_text = Text("Redpanda\n[telemetry-data]", font_size=11, color=WHITE)
        redpanda_text.next_to(redpanda_box, RIGHT, buff=0.3)
        
        self.play(FadeIn(redpanda_box, redpanda_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow to streaming service
        arrow2 = Arrow(redpanda_box.get_right(), np.array([2, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow2), run_time=0.4)
        
        streaming_box = RoundedRectangle(width=2.8, height=0.8, color=TEAL, 
                                        fill_opacity=0.3, stroke_width=2)
        streaming_box.move_to([3.5, 2, 0])
        streaming_text = Text("streaming_service\n(QuixStreams)", font_size=11, color=WHITE)
        streaming_text.move_to(streaming_box)
        
        self.play(FadeIn(streaming_box, streaming_text), run_time=0.6)
        self.wait(0.5)
        
        # Feature computations
        features = [
            {
                "name": "Raw Sink",
                "desc": "LocalFileSink\ndata/entity_df/",
                "y": 0.8
            },
            {
                "name": "Current_Imbalance_Ratio",
                "desc": "Instantaneous 3-phase\nelectrical imbalance",
                "y": 0
            },
            {
                "name": "Vibration_RollingMax_10min",
                "desc": "10-min sliding window\nMax(Vibration_mm_s)",
                "y": -0.8
            },
            {
                "name": "Current_Imbalance_RollingMean_5min",
                "desc": "5-min sliding window\nMean(Current_Imbalance)",
                "y": -1.6
            }
        ]
        
        for feature in features:
            # Arrow from streaming service
            arrow = Arrow(np.array([3.5, 2 - 0.5, 0]), np.array([3.5, feature["y"] + 0.4, 0]), 
                         color=YELLOW, stroke_width=1.5, buff=0.1)
            self.play(FadeIn(arrow), run_time=0.3)
            
            # Feature box
            feat_box = RoundedRectangle(width=4, height=0.7, color=TEAL, 
                                       fill_opacity=0.2, stroke_width=1.5)
            feat_box.move_to([3.5, feature["y"], 0])
            
            feat_name = Text(feature["name"], font_size=12, weight=BOLD, color=CYAN)
            feat_desc = Text(feature["desc"], font_size=9, color=GRAY)
            
            feat_texts = VGroup(feat_name, feat_desc).arrange(DOWN, buff=0.1)
            feat_texts.move_to(feat_box)
            
            self.play(FadeIn(feat_box, feat_texts), run_time=0.4)
            self.wait(0.2)
        
        # Final destination: Feast & Redis
        final_arrow = Arrow(np.array([3.5, -1.6, 0]), np.array([5.5, -1.6, 0]), 
                           color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(final_arrow), run_time=0.4)
        
        final_box = RoundedRectangle(width=2.5, height=0.8, color=GREEN, 
                                    fill_opacity=0.2, stroke_width=2)
        final_box.move_to([6.8, -1.6, 0])
        final_text = Text("Feast\nRedis + Parquet", font_size=11, color=GREEN)
        final_text.move_to(final_box)
        
        self.play(FadeIn(final_box, final_text), run_time=0.6)
        self.wait(1.5)


class InferencePipelineFlow(Scene):
    """Detailed Inference Pipeline"""
    def construct(self):
        title = Text("4. Inference Pipeline", font_size=48, weight=BOLD, color=GREEN)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Start: Redpanda
        redpanda = Circle(radius=0.35, color=BLUE, fill_opacity=0.5)
        redpanda.move_to([-6, 1.5, 0])
        redpanda_text = Text("Redpanda\n[telemetry-data]", font_size=10, color=WHITE)
        redpanda_text.next_to(redpanda, DOWN, buff=0.3)
        
        self.play(FadeIn(redpanda, redpanda_text), run_time=0.5)
        self.wait(0.3)
        
        # Arrow to inference service
        arrow1 = Arrow(redpanda.get_right(), np.array([-4, 1.5, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow1), run_time=0.4)
        
        # Inference service
        inference_box = RoundedRectangle(width=3.5, height=1, color=GREEN, 
                                        fill_opacity=0.3, stroke_width=2)
        inference_box.move_to([-2, 1.5, 0])
        inference_text = Text("inference_service\n(QuixStreams)\nConsumes telemetry data", 
                             font_size=11, color=WHITE)
        inference_text.move_to(inference_box)
        
        self.play(FadeIn(inference_box, inference_text), run_time=0.6)
        self.wait(0.5)
        
        # Feature retrieval from Feast
        arrow2 = Arrow(np.array([-2, 1, 0]), np.array([-2, 0.2, 0]), color=YELLOW, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow2), run_time=0.4)
        
        feast_box = RoundedRectangle(width=3.5, height=1.2, color=CYAN, 
                                    fill_opacity=0.3, stroke_width=2)
        feast_box.move_to([-2, -0.5, 0])
        feast_text = Text("Feast Online Features\nmachine_anomaly_service_v1",
                         font_size=10, color=CYAN)
        feast_text.move_to(feast_box)
        
        self.play(FadeIn(feast_box, feast_text), run_time=0.6)
        self.wait(0.3)
        
        # Features being retrieved
        features = [
            ("Vibration_RollingMax_10min", "Streaming, TTL 15min"),
            ("Current_Imbalance_Ratio", "Streaming, TTL 8min"),
            ("Current_Imbalance_RollingMean_5min", "Streaming, TTL 8min"),
            ("Daily_Vibration_PeakMean_Ratio", "Batch, Daily")
        ]
        
        for i, (feat, source) in enumerate(features):
            feat_text = Text(f"• {feat}", font_size=9, color=WHITE)
            source_text = Text(f"  ({source})", font_size=8, color=GRAY)
            
            feat_text.move_to([-2, -1.2 - i*0.3, 0])
            source_text.next_to(feat_text, RIGHT, buff=0.1)
            
            self.play(Write(feat_text), Write(source_text), run_time=0.4)
            self.wait(0.1)
        
        self.wait(0.5)
        
        # Arrow to model
        arrow3 = Arrow(np.array([-2, -2.4, 0]), np.array([-2, -3.2, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow3), run_time=0.4)
        
        # IsolationForest model
        model_box = RoundedRectangle(width=3.5, height=0.9, color=ORANGE, 
                                    fill_opacity=0.3, stroke_width=2)
        model_box.move_to([-2, -3.7, 0])
        model_text = Text("IsolationForest\nModel Inference\nClassify: Normal vs Anomaly",
                         font_size=10, color=ORANGE)
        model_text.move_to(model_box)
        
        self.play(FadeIn(model_box, model_text), run_time=0.6)
        self.wait(0.5)
        
        # Predictions output
        arrow4 = Arrow(np.array([-2, -4.2, 0]), np.array([-2, -4.8, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow4), run_time=0.4)
        
        pred_box = RoundedRectangle(width=3, height=0.8, color=RED, 
                                   fill_opacity=0.2, stroke_width=2)
        pred_box.move_to([-2, -5.3, 0])
        pred_text = Text("Redpanda\n[predictions]", font_size=11, color=RED)
        pred_text.move_to(pred_box)
        
        self.play(FadeIn(pred_box, pred_text), run_time=0.6)
        self.wait(1)


class AnomalyInvestigationFlow(Scene):
    """Anomaly Investigation Pipeline with RAG"""
    def construct(self):
        title = Text("5. Anomaly Investigation Pipeline", font_size=48, weight=BOLD, color=ORANGE)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Predictions trigger
        trigger_box = RoundedRectangle(width=3, height=0.8, color=RED, 
                                      fill_opacity=0.2, stroke_width=2)
        trigger_box.move_to([-6, 2, 0])
        trigger_text = Text("Redpanda\n[predictions]\nis_anomaly == 1",
                           font_size=10, color=RED)
        trigger_text.move_to(trigger_box)
        
        self.play(FadeIn(trigger_box, trigger_text), run_time=0.5)
        self.wait(0.3)
        
        # if_anomaly_service
        arrow1 = Arrow(trigger_box.get_right(), np.array([-4, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow1), run_time=0.4)
        
        if_anom_box = RoundedRectangle(width=2.8, height=0.8, color=ORANGE, 
                                      fill_opacity=0.3, stroke_width=2)
        if_anom_box.move_to([-2.5, 2, 0])
        if_anom_text = Text("if_anomaly_service\nFilter & Process",
                           font_size=10, color=ORANGE)
        if_anom_text.move_to(if_anom_box)
        
        self.play(FadeIn(if_anom_box, if_anom_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow to LangChain
        arrow2 = Arrow(if_anom_box.get_right(), np.array([0.5, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow2), run_time=0.4)
        
        langchain_box = RoundedRectangle(width=3, height=0.8, color=PURPLE, 
                                        fill_opacity=0.3, stroke_width=2)
        langchain_box.move_to([2, 2, 0])
        langchain_text = Text("langchain_service\nReAct Agent",
                             font_size=10, color=PURPLE)
        langchain_text.move_to(langchain_box)
        
        self.play(FadeIn(langchain_box, langchain_text), run_time=0.6)
        self.wait(0.5)
        
        # MCP Tool call
        arrow3 = Arrow(np.array([2, 1.6, 0]), np.array([2, 0.8, 0]), color=YELLOW, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow3), run_time=0.4)
        
        mcp_box = RoundedRectangle(width=3.5, height=0.9, color=CYAN, 
                                  fill_opacity=0.3, stroke_width=2)
        mcp_box.move_to([2, 0.2, 0])
        mcp_text = Text("MCP Server\nretrieve_context()\nmachine_id, query",
                       font_size=9, color=CYAN)
        mcp_text.move_to(mcp_box)
        
        self.play(FadeIn(mcp_box, mcp_text), run_time=0.6)
        self.wait(0.3)
        
        # RAG components
        self.play(Create(Line(np.array([-4, -0.3, 0]), np.array([6, -0.3, 0]), color=GRAY, stroke_width=1)))
        
        # Qdrant
        arrow4 = Arrow(np.array([2, -0.3, 0]), np.array([0, -1.2, 0]), color=YELLOW, stroke_width=1.5, buff=0.1)
        self.play(FadeIn(arrow4), run_time=0.3)
        
        qdrant_box = RoundedRectangle(width=2.8, height=0.9, color=TEAL, 
                                     fill_opacity=0.2, stroke_width=2)
        qdrant_box.move_to([-1, -1.6, 0])
        qdrant_text = Text("Qdrant\nHybrid Retrieval\n(Dense + BM25)",
                          font_size=9, color=TEAL)
        qdrant_text.move_to(qdrant_box)
        
        self.play(FadeIn(qdrant_box, qdrant_text), run_time=0.6)
        self.wait(0.2)
        
        # MongoDB audit
        arrow5 = Arrow(np.array([2, -0.3, 0]), np.array([4, -1.2, 0]), color=YELLOW, stroke_width=1.5, buff=0.1)
        self.play(FadeIn(arrow5), run_time=0.3)
        
        mongo_box = RoundedRectangle(width=2.8, height=0.9, color=GREEN, 
                                    fill_opacity=0.2, stroke_width=2)
        mongo_box.move_to([5, -1.6, 0])
        mongo_text = Text("MongoDB\nAudit Log\nQuery + Context",
                         font_size=9, color=GREEN)
        mongo_text.move_to(mongo_box)
        
        self.play(FadeIn(mongo_box, mongo_text), run_time=0.6)
        self.wait(0.3)
        
        # Retrieved context back to LangChain
        arrow6 = Arrow(np.array([-1, -2.1, 0]), np.array([0, -2.5, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow6), run_time=0.3)
        
        arrow7 = Arrow(np.array([5, -2.1, 0]), np.array([4, -2.5, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow7), run_time=0.3)
        
        context_box = RoundedRectangle(width=3.5, height=0.8, color=BLUE, 
                                      fill_opacity=0.2, stroke_width=2)
        context_box.move_to([2, -2.9, 0])
        context_text = Text("Retrieved Context\nMachine KB", font_size=10, color=BLUE)
        context_text.move_to(context_box)
        
        self.play(FadeIn(context_box, context_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow to vLLM
        arrow8 = Arrow(np.array([2, -3.3, 0]), np.array([2, -4.1, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow8), run_time=0.4)
        
        vllm_box = RoundedRectangle(width=3.5, height=0.9, color=YELLOW, 
                                   fill_opacity=0.2, stroke_width=2)
        vllm_box.move_to([2, -4.6, 0])
        vllm_text = Text("vLLM\nQwen2.5-7B-Instruct\nGenerate Summary",
                        font_size=9, color=YELLOW)
        vllm_text.move_to(vllm_box)
        
        self.play(FadeIn(vllm_box, vllm_text), run_time=0.6)
        self.wait(0.3)
        
        # Final: Slack notification
        arrow9 = Arrow(np.array([2, -5.1, 0]), np.array([2, -5.9, 0]), color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow9), run_time=0.4)
        
        slack_box = RoundedRectangle(width=3.5, height=0.8, color=PINK, 
                                    fill_opacity=0.3, stroke_width=2)
        slack_box.move_to([2, -6.4, 0])
        slack_text = Text("Slack Notification\nMachine ID + Summary",
                         font_size=10, color=PINK)
        slack_text.move_to(slack_box)
        
        self.play(FadeIn(slack_box, slack_text), run_time=0.6)
        self.wait(1.5)


class BatchPipelineFlow(Scene):
    """Batch Pipeline - Daily"""
    def construct(self):
        title = Text("3. Batch Pipeline (Daily)", font_size=48, weight=BOLD, color=TEAL)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Airflow trigger
        airflow_box = RoundedRectangle(width=3.5, height=0.9, color=ORANGE, 
                                      fill_opacity=0.3, stroke_width=2)
        airflow_box.move_to([-5.5, 2, 0])
        airflow_text = Text("Airflow Scheduler\ndaily_batch_feature_pipeline\n00:00 UTC",
                           font_size=10, color=ORANGE)
        airflow_text.move_to(airflow_box)
        
        self.play(FadeIn(airflow_box, airflow_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow
        arrow1 = Arrow(airflow_box.get_right(), np.array([-3.5, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow1), run_time=0.4)
        
        # Batch service
        batch_box = RoundedRectangle(width=3.5, height=1, color=TEAL, 
                                    fill_opacity=0.3, stroke_width=2)
        batch_box.move_to([-1.5, 2, 0])
        batch_text = Text("batch_pipeline_service\nDockerOperator",
                         font_size=10, color=TEAL)
        batch_text.move_to(batch_box)
        
        self.play(FadeIn(batch_box, batch_text), run_time=0.6)
        self.wait(0.3)
        
        # Operations
        operations = [
            ("Read", "data/entity_df/ (raw telemetry)", 0.5),
            ("Compute", "Daily_Vibration_PeakMean_Ratio\n(PySpark, 1-day window)", -0.7),
            ("Write", "data/offline/machines_batch_features/", -2),
            ("Materialize", "feast.materialize_incremental() → Redis", -3.3)
        ]
        
        for op_name, op_desc, y_pos in operations:
            arrow = Arrow(np.array([1.5, 2 - 0.5, 0]), np.array([3, y_pos + 0.3, 0]), 
                         color=YELLOW, stroke_width=1.5, buff=0.1)
            self.play(FadeIn(arrow), run_time=0.3)
            
            op_box = RoundedRectangle(width=4.5, height=0.8, color=TEAL, 
                                     fill_opacity=0.2, stroke_width=1.5)
            op_box.move_to([5.5, y_pos, 0])
            
            op_title = Text(op_name, font_size=12, weight=BOLD, color=CYAN)
            op_info = Text(op_desc, font_size=9, color=GRAY)
            
            op_texts = VGroup(op_title, op_info).arrange(DOWN, buff=0.1)
            op_texts.move_to(op_box)
            
            self.play(FadeIn(op_box, op_texts), run_time=0.5)
            self.wait(0.2)
        
        self.wait(1)


class RetrainingPipelineFlow(Scene):
    """Retraining Pipeline - Weekly"""
    def construct(self):
        title = Text("6. Retraining Pipeline (Weekly)", font_size=48, weight=BOLD, color=PINK)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Airflow trigger
        airflow_box = RoundedRectangle(width=3.5, height=1, color=ORANGE, 
                                      fill_opacity=0.3, stroke_width=2)
        airflow_box.move_to([-5.5, 2, 0])
        airflow_text = Text("Airflow Scheduler\nweekly_retraining\nMonday 02:00 UTC",
                           font_size=10, color=ORANGE)
        airflow_text.move_to(airflow_box)
        
        self.play(FadeIn(airflow_box, airflow_text), run_time=0.6)
        self.wait(0.3)
        
        # Arrow
        arrow1 = Arrow(airflow_box.get_right(), np.array([-3.5, 2, 0]), 
                      color=WHITE, stroke_width=2, buff=0.1)
        self.play(FadeIn(arrow1), run_time=0.4)
        
        # Retraining service
        retrain_box = RoundedRectangle(width=3.5, height=1, color=PINK, 
                                      fill_opacity=0.3, stroke_width=2)
        retrain_box.move_to([-1.5, 2, 0])
        retrain_text = Text("retraining_service\nDockerOperator",
                           font_size=10, color=PINK)
        retrain_text.move_to(retrain_box)
        
        self.play(FadeIn(retrain_box, retrain_text), run_time=0.6)
        self.wait(0.3)
        
        # Steps
        steps = [
            ("Load", "entity_df (raw Parquet)", 0.5),
            ("Join", "Feast point-in-time join\nFull historical features", -0.5),
            ("Fit", "New IsolationForest Pipeline\n(subsample ≤ 50k rows)", -1.6),
            ("Eval", "Full dataset evaluation\n(10k rows/chunk)", -2.7),
            ("Register", "MLflow registry\nNew model version", -3.8),
            ("Output", "thresholds.json\nShared volume", -4.9)
        ]
        
        for step_name, step_desc, y_pos in steps:
            arrow = Arrow(np.array([1.5, 2 - 0.5, 0]), np.array([3.2, y_pos + 0.3, 0]), 
                         color=YELLOW, stroke_width=1.5, buff=0.1)
            self.play(FadeIn(arrow), run_time=0.25)
            
            step_box = RoundedRectangle(width=4.5, height=0.8, color=PINK, 
                                       fill_opacity=0.2, stroke_width=1.5)
            step_box.move_to([5.7, y_pos, 0])
            
            step_title = Text(step_name, font_size=11, weight=BOLD, color=PINK)
            step_info = Text(step_desc, font_size=8, color=GRAY)
            
            step_texts = VGroup(step_title, step_info).arrange(DOWN, buff=0.08)
            step_texts.move_to(step_box)
            
            self.play(FadeIn(step_box, step_texts), run_time=0.4)
            self.wait(0.15)
        
        self.wait(1)


class FeatureVectorTable(Scene):
    """Feature Vector at Inference Time"""
    def construct(self):
        title = Text("Feature Vector at Inference Time", font_size=48, weight=BOLD, color=CYAN)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Table headers
        headers = ["Feature", "Source", "Cadence", "Description"]
        header_y = 2.5
        
        header_positions = [-4, -0.5, 2.5, 5.5]
        for header, x_pos in zip(headers, header_positions):
            header_text = Text(header, font_size=14, weight=BOLD, color=BLUE)
            header_text.move_to([x_pos, header_y, 0])
            self.play(Write(header_text), run_time=0.3)
        
        # Data rows
        features = [
            {
                "name": "Vibration_RollingMax_10min",
                "source": "Streaming",
                "cadence": "Every message",
                "desc": "Max vibration over last 10 min"
            },
            {
                "name": "Current_Imbalance_Ratio",
                "source": "Streaming",
                "cadence": "Every message",
                "desc": "3-phase electrical imbalance"
            },
            {
                "name": "Current_Imbalance_RollingMean_5min",
                "source": "Streaming",
                "cadence": "Every message",
                "desc": "Smoothed imbalance over 5 min"
            },
            {
                "name": "Daily_Vibration_PeakMean_Ratio",
                "source": "Batch",
                "cadence": "Daily",
                "desc": "Peak/mean vibration for full day"
            }
        ]
        
        row_y = 1.5
        for i, feature in enumerate(features):
            # Row background
            row_bg = Rectangle(width=20, height=0.6, 
                             color=BLUE if i % 2 == 0 else TEAL,
                             fill_opacity=0.1, stroke_width=0)
            row_bg.move_to([0, row_y - i*0.8, 0])
            self.play(FadeIn(row_bg), run_time=0.1)
            
            # Row data
            row_data = [
                feature["name"],
                feature["source"],
                feature["cadence"],
                feature["desc"]
            ]
            
            for data, x_pos in zip(row_data, header_positions):
                data_text = Text(data, font_size=11, color=WHITE)
                data_text.move_to([x_pos, row_y - i*0.8, 0])
                self.play(Write(data_text), run_time=0.2)
            
            self.wait(0.15)
        
        self.wait(1.5)


class ColdStartServices(Scene):
    """Cold Start & Utility Services"""
    def construct(self):
        title = Text("Cold Start & Utility Services", font_size=48, weight=BOLD, color=GREEN)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # cold_start_util
        cold_box = RoundedRectangle(width=8, height=1.8, color=GREEN, 
                                   fill_opacity=0.3, stroke_width=2)
        cold_box.move_to([0, 1.5, 0])
        
        cold_title = Text("cold_start_util", font_size=18, weight=BOLD, color=GREEN)
        cold_desc = Text(
            "On first start: batch pipeline has not yet run & Redis is empty\n"
            "Reads most recent batch features from processed historical datasets\n"
            "Materializes directly into Redis for valid feature vector on first message",
            font_size=12, color=WHITE
        )
        
        cold_texts = VGroup(cold_title, cold_desc).arrange(DOWN, buff=0.2, aligned_edge=LEFT)
        cold_texts.move_to(cold_box)
        
        self.play(FadeIn(cold_box, cold_texts), run_time=0.8)
        self.wait(0.8)
        
        # offline_files_util
        offline_box = RoundedRectangle(width=8, height=1.8, color=TEAL, 
                                      fill_opacity=0.3, stroke_width=2)
        offline_box.move_to([0, -1.5, 0])
        
        offline_title = Text("offline_files_util", font_size=18, weight=BOLD, color=TEAL)
        offline_desc = Text(
            "Creates offline store directory structure expected by Feast\n"
            "Generates: data/offline/ folder before any pipeline writes\n"
            "Prevents FileNotFoundError on first feast apply",
            font_size=12, color=WHITE
        )
        
        offline_texts = VGroup(offline_title, offline_desc).arrange(DOWN, buff=0.2, aligned_edge=LEFT)
        offline_texts.move_to(offline_box)
        
        self.play(FadeIn(offline_box, offline_texts), run_time=0.8)
        self.wait(1)
        
        # Connection arrow
        arrow = Arrow(cold_box.get_bottom(), offline_box.get_top(), 
                     color=WHITE, stroke_width=2, buff=0.3)
        self.play(FadeIn(arrow), run_time=0.5)
        
        # Summary
        summary = Text("Both run automatically on first docker compose up", 
                      font_size=14, color=YELLOW, style=ITALIC)
        summary.move_to([0, -3.5, 0])
        self.play(Write(summary), run_time=0.8)
        
        self.wait(1.5)


class TrainingVsRetraining(Scene):
    """Comparison: Training vs Retraining"""
    def construct(self):
        title = Text("Training vs Retraining", font_size=48, weight=BOLD, color=ORANGE)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Headers
        train_header = Text("training_service", font_size=20, weight=BOLD, color=BLUE)
        train_header.move_to([-3.5, 2.3, 0])
        
        retrain_header = Text("retraining_service", font_size=20, weight=BOLD, color=PINK)
        retrain_header.move_to([3.5, 2.3, 0])
        
        self.play(Write(train_header), Write(retrain_header), run_time=0.6)
        
        # Comparison data
        comparisons = [
            ("Purpose", "Bootstrap — run once", "Periodic update — every Monday"),
            ("Data source", "Processed datalake (Parquet)", "Feast point-in-time join"),
            ("Feast dep.", "None (offline)", "Required (online+offline)"),
            ("MLflow exp.", "isolation_forest_prod", "isolation_forest_retrain"),
            ("Trigger", "Manual / one-shot", "Airflow weekly_retraining DAG"),
        ]
        
        aspect_x = -6.5
        train_x = -3.5
        retrain_x = 3.5
        
        aspect_header = Text("Aspect", font_size=13, weight=BOLD, color=GRAY)
        aspect_header.move_to([aspect_x, 1.6, 0])
        
        train_header2 = Text("Bootstrap", font_size=13, weight=BOLD, color=BLUE)
        train_header2.move_to([train_x, 1.6, 0])
        
        retrain_header2 = Text("Periodic", font_size=13, weight=BOLD, color=PINK)
        retrain_header2.move_to([retrain_x, 1.6, 0])
        
        self.play(Write(aspect_header), Write(train_header2), Write(retrain_header2), run_time=0.5)
        
        # Data rows
        row_y = 0.8
        for i, (aspect, train_val, retrain_val) in enumerate(comparisons):
            aspect_text = Text(aspect, font_size=11, weight=BOLD, color=YELLOW)
            aspect_text.move_to([aspect_x, row_y - i*0.75, 0])
            
            train_text = Text(train_val, font_size=10, color=CYAN)
            train_text.move_to([train_x, row_y - i*0.75, 0])
            
            retrain_text = Text(retrain_val, font_size=10, color=PINK)
            retrain_text.move_to([retrain_x, row_y - i*0.75, 0])
            
            self.play(Write(aspect_text), Write(train_text), Write(retrain_text), run_time=0.5)
            self.wait(0.1)
        
        self.wait(1.5)


class StartupOrder(Scene):
    """Startup Order - Step by step"""
    def construct(self):
        title = Text("Startup Order", font_size=48, weight=BOLD, color=YELLOW)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # One-time setup section
        setup_title = Text("One-Time Setup (Before First Online Run)", 
                          font_size=20, weight=BOLD, color=ORANGE)
        setup_title.move_to([0, 2.3, 0])
        self.play(Write(setup_title), run_time=0.6)
        self.wait(0.3)
        
        setup_steps = [
            ("1", "Create offline store folders", "docker compose run --rm create_offline_files"),
            ("2", "Generate synthetic data", "docker compose run --rm create_datasets"),
            ("3", "Feature engineering", "docker compose run --rm data_engineering"),
            ("4", "Compute batch features", "docker compose run --rm batch_feature_pipeline"),
            ("5", "Register Feast definitions", "docker compose --profile setup run --rm feature_store_apply"),
            ("6", "Initial model training", "docker compose run --rm training_service"),
            ("7", "Build Qdrant knowledge base", "docker compose run --rm ingestion_rag"),
        ]
        
        step_y = 1.5
        for i, (num, desc, cmd) in enumerate(setup_steps):
            # Step box
            step_box = RoundedRectangle(width=10, height=0.7, color=BLUE, 
                                       fill_opacity=0.2, stroke_width=1.5)
            step_box.move_to([0.5, step_y - i*0.85, 0])
            
            # Number
            num_text = Text(num, font_size=14, weight=BOLD, color=YELLOW)
            num_text.move_to([-5, step_y - i*0.85, 0])
            
            # Description
            desc_text = Text(desc, font_size=12, color=CYAN)
            desc_text.move_to([-3, step_y - i*0.85, 0])
            
            # Command
            cmd_text = Text(cmd, font_size=9, color=GREEN, family="monospace")
            cmd_text.move_to([6.5, step_y - i*0.85, 0])
            
            self.play(FadeIn(step_box, num_text, desc_text, cmd_text), run_time=0.4)
            self.wait(0.15)
        
        self.wait(0.5)


class InfrastructureOverview(Scene):
    """Infrastructure at a Glance"""
    def construct(self):
        title = Text("Infrastructure at a Glance", font_size=48, weight=BOLD, color=CYAN)
        title.to_edge(UP, buff=0.3)
        self.play(Write(title))
        self.wait(0.5)
        
        # Infrastructure table
        infra_items = [
            ("Message broker", "Redpanda (Kafka-compatible)", "19092"),
            ("Redpanda Console", "Web UI", "8080"),
            ("Online feature store", "Redis 6.2", "6379"),
            ("Redis Insight", "Web UI", "5540"),
            ("Feature server", "Feast serve", "8000 → 6566"),
            ("ML tracking", "MLflow", "5000"),
            ("Vector database", "Qdrant", "6333, 6334"),
            ("Document database", "MongoDB 7", "27017"),
            ("LLM server", "vLLM (Qwen2.5-7B)", "8222 → 8000"),
            ("MCP server", "FastMCP", "8020"),
            ("LangChain agent", "FastAPI", "8010"),
            ("Airflow", "Web UI", "8081"),
        ]
        
        service_x = -5.5
        tech_x = 1.5
        port_x = 7.5
        
        # Headers
        service_h = Text("Service", font_size=14, weight=BOLD, color=BLUE)
        service_h.move_to([service_x, 2.3, 0])
        
        tech_h = Text("Technology", font_size=14, weight=BOLD, color=BLUE)
        tech_h.move_to([tech_x, 2.3, 0])
        
        port_h = Text("Port", font_size=14, weight=BOLD, color=BLUE)
        port_h.move_to([port_x, 2.3, 0])
        
        self.play(Write(service_h), Write(tech_h), Write(port_h), run_time=0.6)
        self.wait(0.3)
        
        # Data rows
        row_y = 1.5
        for i, (service, tech, port) in enumerate(infra_items):
            # Row background
            row_bg = Rectangle(width=20, height=0.55, 
                             color=CYAN if i % 2 == 0 else TEAL,
                             fill_opacity=0.08, stroke_width=0)
            row_bg.move_to([0, row_y - i*0.65, 0])
            self.play(FadeIn(row_bg), run_time=0.05)
            
            # Data
            service_text = Text(service, font_size=10, color=WHITE)
            service_text.move_to([service_x, row_y - i*0.65, 0])
            
            tech_text = Text(tech, font_size=9, color=GRAY)
            tech_text.move_to([tech_x, row_y - i*0.65, 0])
            
            port_text = Text(port, font_size=10, color=GREEN, family="monospace")
            port_text.move_to([port_x, row_y - i*0.65, 0])
            
            self.play(Write(service_text), Write(tech_text), Write(port_text), run_time=0.25)
            self.wait(0.05)
        
        self.wait(1)


class SystemHighlight(Scene):
    """Key Highlights of the System"""
    def construct(self):
        title = Text("System Design Highlights", font_size=50, weight=BOLD, color=GREEN)
        title.to_edge(UP, buff=0.4)
        self.play(Write(title))
        self.wait(0.5)
        
        highlights = [
            {
                "title": "Dual-Pipeline Feature Store",
                "desc": "Same features computed offline (training) and online (serving)\nEliminates training-serving skew through single versioned contract",
                "color": BLUE,
                "y": 2
            },
            {
                "title": "Real-Time Inference",
                "desc": "Streaming data ingestion at 3 msg/sec\nInference pipeline serves features from Feast in milliseconds",
                "color": CYAN,
                "y": 0.5
            },
            {
                "title": "Event-Driven Investigation",
                "desc": "On anomaly detection: Triggered retrieval from Qdrant RAG\nAutomatic Slack notifications with LLM-generated analysis",
                "color": ORANGE,
                "y": -1
            },
            {
                "title": "Automated Retraining",
                "desc": "Weekly Monday 02:00 UTC via Airflow\nPoint-in-time joins ensure label leakage prevention",
                "color": PINK,
                "y": -2.5
            },
            {
                "title": "Production-Ready Architecture",
                "desc": "Docker containerized all services\nRedis for hot cache, Qdrant for semantic search, MongoDB for audit logs",
                "color": YELLOW,
                "y": -4
            }
        ]
        
        for highlight in highlights:
            box = RoundedRectangle(width=9.5, height=1.2, color=highlight["color"], 
                                  fill_opacity=0.2, stroke_width=2)
            box.move_to([0, highlight["y"], 0])
            
            title_text = Text(highlight["title"], font_size=16, weight=BOLD, 
                            color=highlight["color"])
            desc_text = Text(highlight["desc"], font_size=11, color=WHITE)
            
            texts = VGroup(title_text, desc_text).arrange(DOWN, buff=0.15, aligned_edge=LEFT)
            texts.move_to(box)
            
            self.play(FadeIn(box, texts), run_time=0.7)
            self.wait(0.3)
        
        self.wait(1.5)


class FinalSummary(Scene):
    """Final Summary and Conclusion"""
    def construct(self):
        # Main title
        title = Text("Production MLOps System Complete", font_size=52, weight=BOLD, color=BLUE)
        title.move_to([0, 2.5, 0])
        self.play(Write(title), run_time=1)
        self.wait(0.5)
        
        # Key statistics
        stats = VGroup()
        
        stat_items = [
            ("6 Core Pipelines", "Data Prep, Streaming, Batch, Inference, Investigation, Retraining"),
            ("12+ Services", "Orchestrated via Docker Compose"),
            ("4 Feature Types", "Streaming + Batch features unified through Feast"),
            ("5 Data Stores", "Redis, Qdrant, MongoDB, Redpanda, MLflow"),
            ("Real-Time Anomaly Detection", "IsolationForest with LLM-powered investigation"),
        ]
        
        for i, (stat, detail) in enumerate(stat_items):
            stat_text = Text(f"• {stat}", font_size=18, weight=BOLD, color=CYAN)
            detail_text = Text(f"  {detail}", font_size=14, color=GRAY)
            
            group = VGroup(stat_text, detail_text).arrange(DOWN, buff=0.05, aligned_edge=LEFT)
            stats.add(group)
        
        stats.arrange(DOWN, buff=0.35)
        stats.move_to([0, 0.8, 0])
        
        for stat in stats:
            self.play(Write(stat), run_time=0.6)
            self.wait(0.2)
        
        self.wait(1)
        
        # Final message
        final = Text("Architectural Correctness & Service Connectivity Demonstrated",
                    font_size=18, style=ITALIC, color=GREEN, weight=BOLD)
        final.move_to([0, -2.5, 0])
        
        self.play(Write(final), run_time=1)
        self.wait(2)


# Main execution
if __name__ == "__main__":
    print("Animation scenes created successfully!")
    print("Render with: manim -pql anomaly_detection_animation.py [SceneName]")
    print("Or render all: manim -pql anomaly_detection_animation.py")