package org.acme.jsonObjectMapper;

import java.util.Comparator;

/**
 *
 * @author Magnus
 */
public class ConfigJson extends Message {

    private Condition highestCondition;

    private Condition getHighestPriorityCondition() {
        if (getConditionsList().size() == 1) {
            return getConditionsList().get(0);
        } else {
            return getConditionsList().stream().max(Comparator.comparingDouble(Condition::getPriority)).get();
        }
    }

    private void removeHighestPriorityCondition() {
        for (int i = 0; i < getConditionsList().size(); i++) {
            if (getConditionsList().get(i).getTopic().equals(highestCondition.getTopic())) {
                getConditionsList().remove(i);
            }
        }
    }
}
