package com.lm.flink.gelly.algorithm;

import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * @Classname CaseModel
 * @Description TODO
 * @Date 2021/1/15 15:16
 * @Created by limeng
 */
public class CaseModel {
    @Data
    static class MemRel implements Serializable {

        private static final long serialVersionUID = 1081642665526573544L;
        private Long from;
        private Long to;
        private Double score;
        private Integer typ;

        @Override
        public String toString() {
            return "MemRel{" +
                    "from=" + from +
                    ", to=" + to +
                    ", score=" + score +
                    ", typ=" + typ +
                    '}';
        }
    }
    @Data
    static class GroupMem implements Serializable {
        private static final long serialVersionUID = 7398468243670431553L;
        private Long targetId;
        private Long groupId;
        private Double score;
        private Integer memType;
        private Integer tag;
        private Set<MemRel> rel;
    }

    @Data
    static class MsgScore implements Serializable {

        private static final long serialVersionUID = 463520118476264349L;
        private Long groupId;
        private Long from;
        private Long to;
        private Double score;

        public MsgScore(Long groupId, Long from, Long to, Double score) {
            this.groupId = groupId;
            this.from = from;
            this.to = to;
            this.score = score;
        }
    }
    @Data
    static class GroupVD implements Serializable{
        private static final long serialVersionUID = 7157646701594615703L;
        private Set<MsgScore> accept;
        private Set<MsgScore> sent;
        private Set<Long> ids;

        public GroupVD(Set<MsgScore> accept, Set<MsgScore> sent, Set<Long> ids) {
            this.accept = accept;
            this.sent = sent;
            this.ids = ids;
        }
    }
}
