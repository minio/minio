function log(m, label) {
	msg = $("<li><span>" + m + "</span></li>");
	msg.find("span").addClass(label);
	out = $("#output");
	out.append(msg);
	out.animate({"scrollTop": out[0].scrollHeight}, "fast");
}

$(document).ready(function() {
	$("#incr").click(function() {
		req = {
			method : "Counter.Incr",
			params : {delta: 1},
		};
		log("<- " + JSON.stringify(req), "secondary label");
		$.jsonrpc(req);
	});
	$("#get").click(function() {
		req = {
			method : "Counter.Get",
			params : {},
		};
		log("<- " + JSON.stringify(req), "label");
		$.jsonrpc(req, {
			success : function(result) {
				$("#get").addClass("success");
				setTimeout(function() {
					$("#get").removeClass("success");
				}, 2000);
				log("-> " + JSON.stringify(result), "success label");
			},
			error : function(error) {
				$("#get").addClass("alert");
				setTimeout(function() {
					$("#get").removeClass("alert");
				}, 2000);
				log("-> " + JSON.stringify(error), "alert label");
			},
		});
	});
	$("#nan").click(function() {
		req = {
			method : "Counter.Nan",
			params : {},
		};
		log("<- " + JSON.stringify(req), "label");
		$.jsonrpc(req, {
			success : function(result) {
				$("#nan").addClass("success");
				setTimeout(function() {
					$("#nan").removeClass("success");
				}, 2000);
				log("-> " + JSON.stringify(result), "success label");
			},
			error : function(error) {
				$("#nan").addClass("alert");
				setTimeout(function() {
					$("#nan").removeClass("alert");
				}, 2000);
				log("-> " + JSON.stringify(error), "alert label");
			},
		});
	});
});
