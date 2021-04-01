package org.imsi.queryERAPI.util;

import org.imsi.queryEREngine.imsi.er.QueryEngine;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLException;

@Component
public class Init {

    QueryEngine queryEngine = new QueryEngine();

    @EventListener(ApplicationReadyEvent.class)
    public void startApp() throws IOException, SQLException {
        queryEngine.initialize();

    }

}
