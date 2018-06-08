package com.blokaly.ceres.web.handlers;

import io.undertow.server.HttpServerExchange;
import org.apache.http.HttpStatus;

/**
 * Simple endpoint to health check a service.
 *
 */
public class HealthCheckHandler extends UndertowGetHandler {

  @Override
  public String handlerPath() {
    return "/health";
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    exchange.setStatusCode(HttpStatus.SC_OK);
  }

}
