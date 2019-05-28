package com.xiaoji.duan.nlp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.ansj.domain.Result;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.util.StringUtils;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.TemplateHandler;
import io.vertx.ext.web.templ.thymeleaf.ThymeleafTemplateEngine;

public class MainVerticle extends AbstractVerticle {

	private ThymeleafTemplateEngine thymeleaf = null;
	private MongoClient mongodb = null;
	private AmqpBridge bridge = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		JsonObject config = new JsonObject();
		config.put("host", config().getString("mongo.host", "duan-mongo"));
		config.put("port", config().getInteger("mongo.port", 27017));
		config.put("keepAlive", config().getBoolean("mongo.keepalive", true));
		mongodb = MongoClient.createShared(vertx, config);

		AmqpBridgeOptions options = new AmqpBridgeOptions();
		bridge = AmqpBridge.create(vertx, options);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();

		// 加载自定义字典
		loadUserDictionary();
		
		thymeleaf = ThymeleafTemplateEngine.create(vertx);
		TemplateHandler templatehandler = TemplateHandler.create(thymeleaf);

		ClassLoaderTemplateResolver resolver = new ClassLoaderTemplateResolver();
		resolver.setSuffix(".html");
		resolver.setCacheable(false);
		resolver.setTemplateMode("HTML5");
		resolver.setCharacterEncoding("utf-8");
		thymeleaf.getThymeleafTemplateEngine().setTemplateResolver(resolver);

		Router router = Router.router(vertx);

		Set<HttpMethod> allowedMethods = new HashSet<HttpMethod>();
		allowedMethods.add(HttpMethod.OPTIONS);
		allowedMethods.add(HttpMethod.GET);
		allowedMethods.add(HttpMethod.POST);
		allowedMethods.add(HttpMethod.PUT);
		allowedMethods.add(HttpMethod.DELETE);
		allowedMethods.add(HttpMethod.CONNECT);
		allowedMethods.add(HttpMethod.PATCH);
		allowedMethods.add(HttpMethod.HEAD);
		allowedMethods.add(HttpMethod.TRACE);

		router.route().handler(CorsHandler.create("*")
				.allowedMethods(allowedMethods)
				.allowedHeader("*")
				.allowedHeader("Content-Type"));
		
		StaticHandler staticfiles = StaticHandler.create().setCachingEnabled(false).setWebRoot("static");
		router.route("/nlp/static/*").handler(staticfiles);
		router.route("/nlp").pathRegex("\\/.+\\.json").handler(staticfiles);
		router.route("/nlp").pathRegex("\\/.+\\.js").handler(staticfiles);
		router.route("/nlp").pathRegex("\\/.+\\.css").handler(staticfiles);
		router.route("/nlp").pathRegex("\\/.+\\.map").handler(staticfiles);

		BodyHandler datahandler = BodyHandler.create();
		router.route("/nlp").pathRegex("\\/*").handler(datahandler);

		router.route("/nlp/api/parsetext").produces("application/json").handler(ctx -> this.parsetext(ctx));

		router.route("/nlp/index").handler(ctx -> this.index(ctx));

		router.route("/nlp").pathRegex("\\/[^\\.]*").handler(templatehandler);

		HttpServerOptions option = new HttpServerOptions();
		option.setCompressionSupported(true);

		vertx.createHttpServer(option).requestHandler(router::accept).listen(8080, http -> {
			if (http.succeeded()) {
				startFuture.complete();
				System.out.println("HTTP server started on http://localhost:8080");
			} else {
				startFuture.fail(http.cause());
			}
		});
	}

	private void loadUserDictionary() {
		FileSystem fs = vertx.fileSystem();

		fs.readFile(config().getString("user.defined.dictionary", "userdefined.json"), handler -> {
			if (handler.succeeded()) {
				Buffer result = handler.result();
				
				JsonArray defines = null;
				
				if (result == null || result.length() < 2) {
					defines = new JsonArray();
				} else {
					try {
						defines = result.toJsonArray();
					} catch(Exception e) {
						e.printStackTrace();
						defines = new JsonArray();
					}
				}

				for (int i  = 0; i < defines.size(); i ++) {
					JsonObject def = defines.getJsonObject(i);
					
					String key = def.getString("key", "dic");
					String keyword = def.getString("keyword");
					String nature = def.getString("nature", "userdefine");
					Integer freq = def.getInteger("freq", 1000);
					
					if (keyword == null || StringUtils.isEmpty(keyword)) {
						continue;
					}
					
					Forest forest = DicLibrary.get(key);
					
					if (forest == null) {
						DicLibrary.put(key, "dic", forest);
					}
					
					DicLibrary.insert(key, keyword, nature, freq);
				}
				
				System.out.println("User defined dictionary loaded.");
			}
		});
	}
	
	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + received.body().encode() + "]");
		JsonObject data = received.body().getJsonObject("body");

		String next = data.getJsonObject("context").getString("next");

		String function = data.getJsonObject("context").getString("function", "");
		StringBuffer text = new StringBuffer();
		if (data.getJsonObject("context").getValue("text") instanceof String) {
			text.append(data.getJsonObject("context").getString("text", ""));
		}
		JsonArray texts = data.getJsonObject("context").getJsonArray("texts", new JsonArray());

		if (!texts.isEmpty()) {
			for (int i = 0; i < texts.size(); i++) {
				if (texts.getValue(i) != null)
					text.append(texts.getString(i));
			}
		}
		
		try {
			Future<JsonObject> future = Future.future();
			
			future.setHandler(handler -> {
				if (handler.succeeded()) {
					JsonObject result = handler.result();

					JsonObject nextctx = new JsonObject().put("context", result);

					MessageProducer<JsonObject> producer = bridge.createProducer(next);
					producer.send(new JsonObject().put("body", nextctx));
					System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.size() + "]");
				} else {
					handler.cause().printStackTrace();
				}
			});
			
			if ("ToAnalysis".equals(function)) {
				toAnalysis(future, text.toString());
			}

			if ("NlpAnalysis".equals(function)) {
				nlpAnalysis(future, text.toString());
			}
			
			if ("IndexAnalysis".equals(function)) {
				indexAnalysis(future, text.toString());
			}
			
			if ("DicAnalysis".equals(function)) {
				dicAnalysis(future, text.toString());
			}
			
			if ("*".equals(function)) {
				List<Future<JsonObject>> futuresAll = new LinkedList<Future<JsonObject>>();

				Future<JsonObject> futureToAnanlysis = Future.future();
				futuresAll.add(futureToAnanlysis);
				
				toAnalysis(futureToAnanlysis, text.toString());

				Future<JsonObject> futureNlpAnalysis = Future.future();
				futuresAll.add(futureNlpAnalysis);

				nlpAnalysis(futureNlpAnalysis, text.toString());
				
				Future<JsonObject> futureIndexAnalysis = Future.future();
				futuresAll.add(futureIndexAnalysis);

				indexAnalysis(futureIndexAnalysis, text.toString());
				
				Future<JsonObject> futureDicAnalysis = Future.future();
				futuresAll.add(futureDicAnalysis);

				dicAnalysis(futureDicAnalysis, text.toString());
				
				CompositeFuture.any(Arrays.asList(futuresAll.toArray(new Future[futuresAll.size()])))
				.map(v -> futuresAll.stream().map(Future::result).findFirst())
				.compose(compose -> {
					future.complete(compose.get());
				}, future).completer();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private void toAnalysis(Future<JsonObject> futureToAnanlysis, String text) {

		vertx.executeBlocking((Future<JsonObject> block) -> {
			Result parsed = ToAnalysis.parse(text);

			JsonArray names = collectWords(parsed.toString(), "nr");
			JsonArray times = collectWords(parsed.toString(), "t");
			JsonArray locations = collectWords(parsed.toString(), "ns");

			JsonObject output = new JsonObject().put("function", "ToAnanlysis").put("text", text).put("parsed",
					new JsonObject().put("plain", parsed.toString()).put("names", names).put("times", times)
							.put("locations", locations));

			block.complete(output);
			
			mongodb.insert("nlp_parse_text", output, insert -> {
			});
		}, false, futureToAnanlysis.completer());

	}

	private void nlpAnalysis(Future<JsonObject> futureNlpAnalysis, String text) {
		vertx.executeBlocking((Future<JsonObject> block) -> {
			Result parsed = NlpAnalysis.parse(text);

			JsonArray names = collectWords(parsed.toString(), "nr");
			JsonArray times = collectWords(parsed.toString(), "t");
			JsonArray locations = collectWords(parsed.toString(), "ns");

			JsonObject output = new JsonObject().put("function", "NlpAnalysis").put("text", text).put("parsed",
					new JsonObject().put("plain", parsed.toString()).put("names", names).put("times", times)
							.put("locations", locations));

			block.complete(output);

			mongodb.insert("nlp_parse_text", output, insert -> {
			});
		}, false, futureNlpAnalysis.completer());

	}

	private void indexAnalysis(Future<JsonObject> futureIndexAnalysis, String text) {
		vertx.executeBlocking((Future<JsonObject> block) -> {
			Result parsed = IndexAnalysis.parse(text);

			JsonArray names = collectWords(parsed.toString(), "nr");
			JsonArray times = collectWords(parsed.toString(), "t");
			JsonArray locations = collectWords(parsed.toString(), "ns");

			JsonObject output = new JsonObject().put("function", "IndexAnalysis").put("text", text).put("parsed",
					new JsonObject().put("plain", parsed.toString()).put("names", names).put("times", times)
							.put("locations", locations));

			block.complete(output);

			mongodb.insert("nlp_parse_text", output, insert -> {
			});
		}, false, futureIndexAnalysis.completer());

	}

	private void dicAnalysis(Future<JsonObject> futureDicAnalysis, String text) {
		vertx.executeBlocking((Future<JsonObject> block) -> {
			Result parsed = DicAnalysis.parse(text);

			JsonArray names = collectWords(parsed.toString(), "nr");
			JsonArray times = collectWords(parsed.toString(), "t");
			JsonArray locations = collectWords(parsed.toString(), "ns");

			JsonObject output = new JsonObject().put("function", "DicAnalysis").put("text", text).put("parsed",
					new JsonObject().put("plain", parsed.toString()).put("names", names).put("times", times)
							.put("locations", locations));

			block.complete(output);

			mongodb.insert("nlp_parse_text", output, insert -> {
			});
		}, false, futureDicAnalysis.completer());

	}

	private void parsetext(RoutingContext ctx) {
		String text = ctx.request().getParam("text");

		Future<JsonObject> up = Future.future();
		List<Future<JsonObject>> futuresAll = new LinkedList<Future<JsonObject>>();

		Future<JsonObject> futureToAnanlysis = Future.future();
		futuresAll.add(futureToAnanlysis);
		
		toAnalysis(futureToAnanlysis, text);

		Future<JsonObject> futureNlpAnalysis = Future.future();
		futuresAll.add(futureNlpAnalysis);

		nlpAnalysis(futureNlpAnalysis, text);
		
		Future<JsonObject> futureIndexAnalysis = Future.future();
		futuresAll.add(futureIndexAnalysis);

		indexAnalysis(futureIndexAnalysis, text);
		
		Future<JsonObject> futureDicAnalysis = Future.future();
		futuresAll.add(futureDicAnalysis);

		dicAnalysis(futureDicAnalysis, text);
		
		CompositeFuture.any(Arrays.asList(futuresAll.toArray(new Future[futuresAll.size()])))
		.map(v -> futuresAll.stream().map(Future::result).findFirst())
		.compose(compose -> {
					up.complete(compose.get());
				}, up).completer();

		up.setHandler(ar -> {
			JsonObject output = new JsonObject();
			
			if (ar.succeeded()) {
				if (ar.result() != null)
					output = ar.result();

				ctx.response().putHeader("content-type", "application/json;charset=utf-8").end(output.encode());
			} else {
				ctx.response().putHeader("content-type", "application/json;charset=utf-8").end(output.encode());
			}
		});
	}

	private JsonArray collectWords(String parsed, String nature) {
		JsonArray names = new JsonArray();
		String[] words = parsed.toString().split(",");

		for (String word : words) {
			if ("".equals(word) || !word.contains("/")) {

			} else {
				String[] props = word.split("/");

				if (props.length == 2) {
					if (nature.equals(props[1])) {
						names.add(props[0]);
					}
				}
			}
		}

		return names;
	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"),
				config().getInteger("stomp.server.port", 5672), res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						if (!config().getBoolean("debug", true)) {
							connectStompServer();
						}
					} else {
						subscribeTrigger(config().getString("amq.app.id", "nlp"));
					}
				});
		
	}
	private void index(RoutingContext ctx) {
		ctx.next();
	}
}
