package mermaid

import "fmt"

const defaultMermaidJsUrl = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"
const defaultTitle = "ClickHouse table dependencies graph"

// HtmlOptions represents options for the Html function.
type HtmlOptions struct {
	// Title is the title of the HTML document.
	Title string

	// MermaidJsUrl is the URL of the Mermaid JS library. Optional.
	MermaidJsUrl string
}

// Html generates a full HTML document with the Mermaid flowchart diagram.
func Html(mermaidString string, options HtmlOptions) string {
	var mermaidJsUrl string
	if options.MermaidJsUrl == "" {
		mermaidJsUrl = defaultMermaidJsUrl
	} else {
		mermaidJsUrl = options.MermaidJsUrl
	}
	var title string
	if options.Title == "" {
		title = defaultTitle
	} else {
		title = options.Title
	}

	return fmt.Sprintf(`
<html lang="en">
	<head>
    	<meta charset="UTF-8">
    	<title>ClickHouse table graph - %s</title>
    	<script src="%s"></script>
		<script src="https://d3js.org/d3.v6.min.js"></script> <!-- For zoom and Pan	-->
    	<script>mermaid.initialize({startOnLoad:true});</script>
		<script>  <!-- For zoom and Pan	-->
			window.addEventListener('load', function () {
				var svgs = d3.selectAll(".mermaid svg");
				svgs.each(function() {
					var svg = d3.select(this);
					svg.html("<g>" + svg.html() + "</g>");
					var inner = svg.select("g");
					var zoom = d3.zoom().on("zoom", function(event) {
						inner.attr("transform", event.transform);
					});
					svg.call(zoom);
				});
			});
		</script>
		<style>
			.mermaid svg {
				max-width: 100%%;
				height: 100%%;
			}
		</style>
	</head>
	<body>
<h3>%s</h3>
		<pre class="mermaid">
			%s
		</pre>
	</body>
</html>

`, title, mermaidJsUrl, title, mermaidString)
}
