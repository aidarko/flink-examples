package com.aidarko.broadcast.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 'implements Serializable' is only to please RichSourceFunction inside TopologyTest
 * otherwise no need to declare in that way
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Action implements Serializable {
    private String actionId;
    private String userId;
    private String action;
}
