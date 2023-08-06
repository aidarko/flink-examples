package com.aidarko.broadcast.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 'implements Serializable' is only to please RichSourceFunction inside TopologyTest
 * otherwise no need to declare in that way
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Rule implements Serializable {
    private String ruleId;
    private List<String> allowedActions;
}
