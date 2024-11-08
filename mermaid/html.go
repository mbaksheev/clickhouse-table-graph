package mermaid

import "fmt"

const defaultMermaidJsUrl = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"
const defaultTitle = "ClickHouse table graph"

type HtmlOptions struct {
	Title        string
	MermaidJsUrl string
}

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
	</head>
	<body>
		<pre class="mermaid">
			%s
		</pre>
	</body>
</html>

`, title, mermaidJsUrl, mermaidString)
}
