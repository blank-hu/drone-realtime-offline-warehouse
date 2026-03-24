package com.rtw.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 控制建议（闭环接口）：
 * - Flink 输出到 Kafka topic：control_suggestion
 * - 仿真侧未来可选择消费执行（当前阶段不需要改仿真端）
 *
 * 设计目标：
 * 1) 可解释：action + reason
 * 2) 幂等友好：suggestion_id 稳定（允许 at-least-once 重复）
 * 3) 最小实现先支持：DELAY_DRONE
 */
public class ControlSuggestion {

    // envelope
    public String schema_version = "ctrl_v1";
    public String event_type = "control_suggestion";

    /** 例如：DELAY_DRONE / TUNE_PARAM */
    public String type;

    // 关联维度
    public String run_id;
    public Long frame_t_ms;

    // 目标对象（最小实现：一个 target）
    public List<String> targets;

    // 动作与原因
    public Action action;
    public Reason reason;

    // 幂等/追踪
    public String suggestion_id;
    public Long emit_ms;

    // 可选：附带实验维度，便于离线评估/聚合
    public String scenario_id;
    public String strategy_id;
    public String strategy_version;
    public String param_set_id;
    public Integer seed;

    public ControlSuggestion() {}

    /** 从碰撞命中生成一个最小的延迟建议（可复现、可解释）。 */
    public static ControlSuggestion delayFromCollisionHit(CollisionHit h, long delayMs, int delayFrames) {
        ControlSuggestion s = new ControlSuggestion();
        s.type = "DELAY_DRONE";

        s.run_id = h.run_id;
        s.frame_t_ms = h.frame_t_ms;

        // 稳定选择目标：字典序更大的那架（保证幂等与可复现）
        String target = (h.drone_a.compareTo(h.drone_b) > 0) ? h.drone_a : h.drone_b;
        s.targets = new ArrayList<>();
        s.targets.add(target);

        Action a = new Action();
        a.delay_ms = delayMs;
        a.delay_frames = delayFrames;
        s.action = a;

        Reason r = new Reason();
        r.kind = "collision_hit";
        r.dist = h.dist;
        r.d_min = h.d_min;
        r.drone_a = h.drone_a;
        r.drone_b = h.drone_b;
        s.reason = r;

        // 维度透传（有则带）
        s.scenario_id = h.scenario_id;
        s.strategy_id = h.strategy_id;
        s.strategy_version = h.strategy_version;
        s.param_set_id = h.param_set_id;
        s.seed = h.seed;

        s.emit_ms = System.currentTimeMillis();
        s.suggestion_id = s.run_id + "|" + s.frame_t_ms + "|DELAY|" + target;

        return s;
    }

    public static class Action {
        // DELAY_DRONE
        public Long delay_ms;
        public Integer delay_frames;

        // TUNE_PARAM（未来扩展）
        public String param;
        public String op;
        public Double value;

        public Action() {}
    }

    public static class Reason {
        public String kind;

        // collision_hit
        public Double dist;
        public Double d_min;
        public String drone_a;
        public String drone_b;

        // congestion_refined（未来扩展）
        public Integer risk_score;
        public Integer risk_pairs;

        public Reason() {}
    }
}
