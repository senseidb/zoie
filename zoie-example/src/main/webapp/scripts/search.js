function handler(errorString, exception)
{
  alert(errorString);
}

DWREngine.setErrorHandler(handler);

// doSearch method
function doSearch()
{
  // var searchRequest = new Object();
  var searchRequest = { query:null};
  searchRequest.query = dwr.util.getValue("query");
  SearchService.search(searchRequest, handleDoSearch);
}

function removeChildren(cell)
{		    	
	if ( cell.hasChildNodes() )
	{
	    while ( cell.childNodes.length >= 1 )
	    {
	        cell.removeChild( cell.firstChild );       
	    } 
	}
}


function renderDoc(doc, elem)
{
    var flds=doc.fields;
	var path=flds.path[0];
	var frag=flds.fragment[0];
	var name;
	var idx=path.lastIndexOf("/");
	if (idx==-1){
		name=path;
	}
	else{
		name=path.substring(idx+1);
	}
	
	var score=doc.score.toFixed(5);
	
	var nameStr="<a class=\"hitlink\" href=\"file:///"+path+"\">"+name+"</a>";
	var fragStr="<div class=\"frag\">"+frag+"</div>";
	var pathStr="<span class=\"path\">"+path+"</span>";
	var scoreStr="<span class=\"score\">score: "+score+"</span>";
	
	elem.innerHTML=nameStr+fragStr+"<div>"+pathStr+" - "+scoreStr+"</div>";
}
		    
function handleDoSearch(result)
{
   $("#hitcount").show().html("<b>"+result.totalHits+"</b> / <b>"+result.totalDocs+"</b>");
   $("#time").show().html("<b>"+result.time+"</b>");
		//		var elem=document.getElementById("results");
		
   var elem=document.getElementById("results");
				
	var table=document.createElement("table");
	table.width="100%";
	table.id="resTable";
	var docs=result.hits;
	
	removeChildren(elem);
	
	for (var i=0;i<docs.length;++i)
	{
		var row=document.createElement("tr");
		table.appendChild(row);
		var doc=docs[i];
		
		var col=document.createElement("td");
		row.appendChild(col);
		
		renderDoc(doc,col);
	}
	elem.appendChild(table);
}

