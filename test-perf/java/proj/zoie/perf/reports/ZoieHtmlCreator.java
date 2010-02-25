package proj.zoie.perf.reports;

import org.deepak.performance.PerformanceManager;
import org.deepak.util.GenericStatisticsUtil;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

public class ZoieHtmlCreator
{

  private List          tableList   = new ArrayList();
  private static String _dateString = null;

  static
  {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
    Date date = new Date(System.currentTimeMillis());
    _dateString = simpleDateFormat.format(date);
  }

  public static String getTimeStamp()
  {
    return _dateString;
  }

  public static String getSortingHtmlBegin(String pageTitle)
  {
    String jsStr =
        "<script type=\"text/javascript\">//<![CDATA[ \n "
            + "           var sourceTable, destTable; \n "
            + "           onload = function() { \n "
            + "               sourceTable = new SortedTable('s'); \n "
            + "               destTable = new SortedTable('d'); \n "
            + "               mySorted = new SortedTable(); \n "
            + "               mySorted.colorize = function() { \n "
            + "                   for (var i=0;i<this.elements.length;i++) { \n "
            + "                       if (i%2){ \n "
            + "                           this.changeClass(this.elements[i],'even','odd'); \n "
            + "                       } else { \n "
            + "                           this.changeClass(this.elements[i],'odd','even'); \n "
            + "                       } \n "
            + "                   } \n "
            + "               } \n "
            + "               mySorted.onsort = mySorted.colorize; \n "
            + "               mySorted.onmove = mySorted.colorize; \n "
            + "               mySorted.colorize(); \n "
            + "               secondTable = SortedTable.getSortedTable(document.getElementById('id')); \n "
            + "               secondTable.allowDeselect = false; \n "
            + "           } \n "
            + "           function moveRows(s,d) { \n "
            + "               var a = new Array(); \n "
            + "               for (var o in s.selectedElements) { \n "
            + "                   a.push(s.selectedElements[o]); \n "
            + "               } \n "
            + "               for (var o in a) { \n "
            + "                   var elm = a[o]; \n "
            + "                   var tds = elm.getElementsByTagName('td'); \n "
            + "                   for (var i in tds) { \n "
            + "                       if (tds[i].headers) tds[i].headers = d.table.id+''+tds[i].headers.substr(d.table.id.length); \n "
            + "                   } \n "
            + "                   d.body.appendChild(a[o]); \n "
            + "                   d.deselect(a[o]); \n "
            + "                   d.init(d.table); \n "
            + "                   d.sort(); \n "
            + "                   s.deselect(a[o]); \n "
            + "                   s.init(s.table); \n " + "               } \n "
            + "           } \n " + "       //]]></script> \n ";

    String sortedTableJs =
        "<script type=\"text/javascript\"> \n "
            + " \n "
            + "SortedTable = function(id) { \n "
            + "   this.table = null; \n "
            + "   if (!document.getElementById || !document.getElementsByTagName) return false; \n "
            + "   if (id) this.init(document.getElementById(id)); \n "
            + "   else this.init(this.findTable()); \n "
            + "   this.prep(); \n "
            + "   if (!id && this.findTable()) new SortedTable(); \n "
            + "} \n "
            + " \n "
            + "// static \n "
            + "SortedTable.tables = new Array(); \n "
            + "SortedTable.move = function(d,elm) { \n "
            + "   var st = SortedTable.getSortedTable(elm); \n "
            + "   if (st) st.move(d,elm); \n "
            + "} \n "
            + "SortedTable.moveSelected = function(d,elm) { \n "
            + "   var st = SortedTable.getSortedTable(elm); \n "
            + "   if (st) st.move(d); \n "
            + "} \n "
            + "SortedTable.findParent = function(elm,tag) { \n "
            + "   while (elm && elm.tagName && elm.tagName.toLowerCase()!=tag) elm = elm.parentNode; \n "
            + "   return elm; \n "
            + "} \n "
            + "SortedTable.getEventElement = function(e) { \n "
            + "   if (!e) e = window.event; \n "
            + "   return (e.target)? e.target : e.srcElement; \n "
            + "} \n "
            + "SortedTable.getSortedTable = function(elm) { \n "
            + "   elm = SortedTable.findParent(elm,'table'); \n "
            + "   for (var i=0;i<SortedTable.tables.length;i++) { \n "
            + "       var t = SortedTable.tables[i].table; \n "
            + "       if (t==elm) return SortedTable.tables[i]; \n "
            + "   } \n "
            + "   return null; \n "
            + "} \n "
            + "SortedTable.gecko = (navigator.product==\"Gecko\"); \n "
            + "SortedTable.removeBeforeSort = SortedTable.gecko; \n "
            + " \n "
            + "// dynamic \n "
            + "SortedTable.prototype = { \n "
            + "// before init finished \n "
            + "   init:function(elm) { \n "
            + "       if (!elm) return false; \n "
            + "       // main DOM properties \n "
            + "       this.table = elm; \n "
            + "       this.head = elm.getElementsByTagName('thead')[0]; \n "
            + "       this.body = elm.getElementsByTagName('tbody')[0]; \n "
            + "       this.foot = elm.getElementsByTagName('tfoot')[0]; \n "
            + "       if (this.hasClass(this.table,'regroup')) this.regroup(); \n "
            + "       this.elements = this.body.getElementsByTagName('tr'); \n "
            + "       // other properties \n "
            + "       this.allowMultiple = true; // set this to false to disallow multiple selection \n "
            + "       this.allowDeselect = true; // set this to false to disallow deselection \n "
            + "       // prepare the table \n "
            + "       this.parseCols(); \n "
            + "       this.selectedElements = new Array(); \n "
            + "   }, \n "
            + "   findTable:function() { \n "
            + "       var elms = document.getElementsByTagName('table'); \n "
            + "       for (var i=0;i<elms.length;i++) { \n "
            + "           if (this.hasClass(elms[i],'sorted') && !SortedTable.getSortedTable(elms[i])) return elms[i]; \n "
            + "       } \n "
            + "       return null; \n "
            + "   }, \n "
            + "   parseCols:function() { \n "
            + "       if (!this.table) return; \n "
            + "       this.cols = new Array(); \n "
            + "       var ths = this.head.getElementsByTagName('th'); \n "
            + "       for (var i=0;i<ths.length;i++) { \n "
            + "           this.cols[ths[i].id] = new Array(); \n "
            + "       } \n "
            + "       for (var i=0;i<this.elements.length;i++) { \n "
            + "           var tds = this.elements[i].getElementsByTagName('td'); \n "
            + "           for (var j=0;j<tds.length;j++) { \n "
            + "               var headers = tds[j].headers.split(' '); \n "
            + "               for (var k=0;k<headers.length;k++) { \n "
            + "                   if (this.cols[headers[k]]) this.cols[headers[k]].push(tds[j]); \n "
            + "               } \n "
            + "           } \n "
            + "       } \n "
            + "   }, \n "
            + "   prep:function() { \n "
            + "       if (!this.table || SortedTable.getSortedTable(this.table)) return; \n "
            + "       this.register(); \n "
            + "       this.prepBody(); \n "
            + "       this.prepHeader(); \n "
            + "   }, \n "
            + "   register:function() { \n "
            + "       SortedTable.tables.push(this); \n "
            + "   }, \n "
            + "   regroup:function() { \n "
            + "       var tbs = this.table.getElementsByTagName('tbody'); \n "
            + "       for (var i=tbs.length-1;i>0;i--) { \n "
            + "           var trs = tbs[i].getElementsByTagName('tr'); \n "
            + "           for (var j=trs.length-1;j>=0;j--) { \n "
            + "               this.body.appendChild(trs[j]); \n "
            + "           } \n "
            + "           this.table.removeChild(tbs[i]); \n "
            + "       } \n "
            + "   }, \n "
            + "// helpers \n "
            + "   trim:function(str) { \n "
            + "       while (str.substr(0,1)==' ') str = str.substr(1); \n "
            + "       while (str.substr(str.length-1,1)==' ') str = str.substr(0,str.length-1); \n "
            + "       return str; \n "
            + "   }, \n "
            + "   hasClass:function(elm,findclass) { \n "
            + "       if (!elm) return null; \n "
            + "       return (' '+elm.className+' ').indexOf(' '+findclass+' ')+1; \n "
            + "   }, \n "
            + "   changeClass:function(elm,oldclass,newclass) { \n "
            + "       if (!elm) return null; \n "
            + "       var c = elm.className.split(' '); \n "
            + "       for (var i=0;i<c.length;i++) { \n "
            + "           c[i] = this.trim(c[i]); \n "
            + "           if (c[i]==oldclass || c[i]==newclass || c[i]=='') c.splice(i,1); \n "
            + "       } \n "
            + "       c.push(newclass); \n "
            + "       elm.className = this.trim(c.join(' ')); \n "
            + "   }, \n "
            + "   elementIndex:function(elm) { \n "
            + "       for (var i=0;i<this.elements.length;i++) { \n "
            + "           if (this.elements[i]==elm) return i; \n "
            + "       } \n "
            + "       return -1; \n "
            + "   }, \n "
            + "   findParent:SortedTable.findParent, \n "
            + "// events \n "
            + "   callBodyHover:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onbodyhover)=='function') st.onbodyhover(elm,e); \n "
            + "       var elm = st.findParent(elm,'tr'); \n "
            + "       if (e.type=='mouseover') st.changeClass(elm,'','hover'); \n "
            + "       else if (e.type=='mouseout') st.changeClass(elm,'hover',''); \n "
            + "       return false; \n "
            + "   }, \n "
            + "   callBodyClick:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onbodyclick)=='function') st.onbodyclick(elm,e); \n "
            + "       var elm = st.findParent(elm,'tr'); \n "
            + "       if (e.shiftKey && st.allowMultiple) st.selectRange(elm); \n "
            + "       else { \n "
            + "           if (st.selected(elm)) { \n "
            + "               if  (st.allowDeselect) st.deselect(elm); \n "
            + "           } else { \n "
            + "               if (!e.ctrlKey || !st.allowMultiple) st.cleanselect(); \n "
            + "               st.select(elm); \n "
            + "           } \n "
            + "       } \n "
            + "       return false; \n "
            + "   }, \n "
            + "   callBodyDblClick:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onbodydblclick)=='function') st.onbodydblclick(elm,e); \n "
            + "       return false; \n "
            + "   }, \n "
            + "   callHeadHover:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onheadhover)=='function') st.onheadhover(elm,e); \n "
            + "       return false; \n "
            + "   }, \n "
            + "   callHeadClick:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onheadclick)=='function') st.onheadclick(elm,e); \n "
            + "       var elm = st.findParent(elm,'th'); \n "
            + "       st.resort(elm); \n "
            + "       return false; \n "
            + "   }, \n "
            + "   callHeadDblClick:function(e) { \n "
            + "       var elm = SortedTable.getEventElement(e); \n "
            + "       var st = SortedTable.getSortedTable(elm); \n "
            + "       if (!st) return false; \n "
            + "       if (typeof(st.onheaddblclick)=='function') st.onheaddblclick(elm,e); \n "
            + "       return false; \n "
            + "   }, \n "
            + "// inited \n "
            + "   prepHeader:function() { \n "
            + "       var ths = this.head.getElementsByTagName('th'); \n "
            + "       for (var i=0;i<ths.length;i++) { \n "
            + "           if (this.hasClass(ths[i],'nosort')) continue; \n "
            + "           ths[i].style.cursor = 'pointer'; \n "
            + "           addEvent(ths[i],'click',this.callHeadClick); \n "
            + "           addEvent(ths[i],'dblclick',this.callHeadDblClick); \n "
            + "           addEvent(ths[i],'mouseover',this.callHeadHover); \n "
            + "           addEvent(ths[i],'mouseout',this.callHeadHover); \n "
            + "           if (this.hasClass(ths[i],'sortedplus') || this.hasClass(ths[i],'sortedminus')) this.sort(ths[i]); \n "
            + "       } \n "
            + "   }, \n "
            + "   prepBody:function() { \n "
            + "       var elm = this.body.lastChild; \n "
            + "       var pelm; \n "
            + "       while (elm) { \n "
            + "           pelm = elm.previousSibling; \n "
            + "           if (elm.nodeType!=1) this.body.removeChild(elm); \n "
            + "           elm = pelm; \n "
            + "       } \n "
            + "       var trs = this.body.getElementsByTagName('tr'); \n "
            + "       for (var i=0;i<trs.length;i++) { \n "
            + "           trs[i].style.cursor = 'pointer'; \n "
            + "           addEvent(trs[i],'click',this.callBodyClick); \n "
            + "           addEvent(trs[i],'dblclick',this.callBodyDblClick); \n "
            + "           addEvent(trs[i],'mouseover',this.callBodyHover); \n "
            + "           addEvent(trs[i],'mouseout',this.callBodyHover); \n "
            + "       } \n "
            + "   }, \n "
            + "// selecting \n "
            + "   selected:function(elm) { \n "
            + "       return this.hasClass(elm,'selrow'); \n "
            + "   }, \n "
            + "   select:function(elm) { \n "
            + "       this.changeClass(elm,'','selrow'); \n "
            + "       this.selectedElements.push(elm); \n "
            + "       if (typeof(this.onselect)=='function') this.onselect(elm); \n "
            + "   }, \n "
            + "   deselect:function(elm) { \n "
            + "       this.changeClass(elm,'selrow',''); \n "
            + "       for (var i=0;i<this.selectedElements.length;i++) { \n "
            + "           if (this.selectedElements[i]==elm) this.selectedElements.splice(i,1); \n "
            + "       } \n "
            + "       if (typeof(this.ondeselect)=='function') this.ondeselect(elm); \n "
            + "   }, \n "
            + "   selectRange:function(elm1) { \n "
            + "       if (this.selectedElements.length==0) { \n "
            + "           this.select(elm1); \n "
            + "           return false; \n "
            + "       } \n "
            + "       var elm0 = this.selectedElements[this.selectedElements.length-1]; \n "
            + "       var d = (this.elementIndex(elm0) < this.elementIndex(elm1)); \n "
            + "       var elm = elm0; \n "
            + "       if (this.selected(elm1)) {if (this.selected(elm0)) this.deselect(elm0);} \n "
            + "       else {if (!this.selected(elm0)) this.select(elm0);} \n "
            + "       do { \n "
            + "           elm = (d)? elm.nextSibling : elm.previousSibling; \n "
            + "           if (this.selected(elm)) this.deselect(elm); \n "
            + "           else this.select(elm); \n "
            + "       } while (elm!=elm1); \n "
            + "       return true; \n "
            + "   }, \n "
            + "   cleanselect:function() { \n "
            + "       for (var i=0;i<this.elements.length;i++) { \n "
            + "           if (this.selected(this.elements[i])) this.deselect(this.elements[i]); \n "
            + "       } \n "
            + "       this.selectedElements = new Array(); \n "
            + "   }, \n "
            + "// sorting \n "
            + "   compareSmart:function(v1,v2) { \n "
            + "       v1 = (v1)? v1.split(' ') : []; \n "
            + "       v2 = (v2)? v2.split(' ') : []; \n "
            + "       l = Math.max(v1.length,v2.length); \n "
            + "       var r = 0; \n "
            + "       for (var i=0;i<l;i++) { \n "
            + "           if (v1[i]==v2[i]) continue; \n "
            + "           if (!v1[i]) v1[i] = \"\"; \n "
            + "           if (!v2[i]) v2[i] = \"\"; \n "
            + "           if (!isNaN(parseFloat(v1[i]))) v1[i] = parseFloat(v1[i]); \n "
            + "           if (!isNaN(parseFloat(v2[i]))) v2[i] = parseFloat(v2[i]); \n "
            + "           if (isNaN(v1[i])&&!isNaN(v2[i])) return 1; \n "
            + "           else if (!isNaN(v1[i])&&isNaN(v2[i])) return -1; \n "
            + "           else if (v1[i]>v2[i]) return 1; \n "
            + "           else if (v1[i]<v2[i]) return -1; \n "
            + "       } \n "
            + "       return 0; \n "
            + "   }, \n "
            + "   compare:function(v1,v2,st) { \n "
            + "       var st = (!st)? SortedTable.getSortedTable(v1) : st; \n "
            + "       if (v1==null || v2==null) return 0; \n "
            + "       var axis = v1.axis.toLowerCase(); \n "
            + "       var v1s = (v1.title)? v1.title : (v1.innerHTML)? v1.innerHTML : ''; \n "
            + "       var v2s = (v2.title)? v2.title : (v2.innerHTML)? v2.innerHTML : ''; \n "
            + "       if (axis=='string') { \n "
            + "           return st.compareSmart(v1s.toLowerCase(),v2s.toLowerCase()); \n "
            + "       } else if (axis=='sstring') { \n "
            + "           return st.compareSmart(v1s,v2s); \n "
            + "       } else if (axis=='number') { \n "
            + "           v1 = parseFloat(v1s); \n "
            + "           if (isNaN(v1)) v1 = Infinity; \n "
            + "           v2 = parseFloat(v2s); \n "
            + "           if (isNaN(v2)) v2 = Infinity; \n "
            + "       } else { \n "
            + "           v1 = (v1s!='')? v1s : v1; \n "
            + "           v2 = (v2s!='')? v2s : v2; \n "
            + "       } \n "
            + "       if (v1==null || v2==null) return 0; \n "
            + "       else if (v1>v2) return 1 \n "
            + "       else if (v1<v2) return -1; \n "
            + "       return 0; \n "
            + "   }, \n "
            + "   findSort:function() { \n "
            + "       var ths = this.head.getElementsByTagName('th'); \n "
            + "       for (var i=0;i<ths.length;i++) { \n "
            + "           if (this.hasClass(ths[i],'sortedminus') || this.hasClass(ths[i],'sortedplus')) return ths[i]; \n "
            + "       } \n "
            + "       return null; \n "
            + "   }, \n "
            + "   sort:function(elm,reverseonly) { \n "
            + "       var st = this; \n "
            + "       var comparator = function(v1,v2) { \n "
            + "           return st.compare(v1,v2,st); \n "
            + "       } \n "
            + "       if (!elm) elm = this.findSort(); \n "
            + "       if (!elm) return false; \n "
            + "       var col = this.cols[elm.id]; \n "
            + "       if (!reverseonly) col.sort(comparator); \n "
            + "       if (this.hasClass(elm,'sortedminus') || reverseonly) col.reverse(); \n "
            + "       var b_sibling,b_parent; \n "
            + "       if (SortedTable.removeBeforeSort) { \n "
            + "           b_sibling = this.body.nextSibling; \n "
            + "           b_parent = this.body.parentNode; \n "
            + "           b_parent.removeChild(this.body); \n "
            + "       } \n "
            + "       for (var i=0;i<col.length;i++) { \n "
            + "           this.body.appendChild(this.findParent(col[i],'tr')); \n "
            + "       } \n "
            + "       if (SortedTable.removeBeforeSort) { \n "
            + "           b_parent.insertBefore(this.body,b_sibling); \n "
            + "       } \n "
            + "       if (typeof(this.onsort)=='function') this.onsort(elm); \n "
            + "   }, \n "
            + "   resort:function(elm) { \n "
            + "       if (!elm) return false; \n "
            + "       this.cleansort(elm); \n "
            + "       var reverseonly = false; \n "
            + "       if (this.hasClass(elm,'sortedplus')) { \n "
            + "           this.changeClass(elm,'sortedplus','sortedminus'); \n "
            + "           reverseonly = true; \n "
            + "       } else if (this.hasClass(elm,'sortedminus')) { \n "
            + "           this.changeClass(elm,'sortedminus','sortedplus'); \n "
            + "           reverseonly = true; \n "
            + "       } else { \n "
            + "           this.changeClass(elm,'sortedminus','sortedplus'); \n "
            + "       } \n "
            + "       this.sort(elm,reverseonly); \n "
            + "   }, \n "
            + "   cleansort:function(except) { \n "
            + "       var ths = this.head.getElementsByTagName('th'); \n "
            + "       for (var i=0;i<ths.length;i++) { \n "
            + "           if (ths[i]==except) continue; \n "
            + "           if (this.hasClass(ths[i],'sortedminus')) this.changeClass(ths[i],'sortedminus',''); \n "
            + "           else if (this.hasClass(ths[i],'sortedplus')) this.changeClass(ths[i],'sortedplus',''); \n "
            + "       } \n "
            + "   }, \n "
            + "// movement \n "
            + "   compareindex:function(v1,v2) { \n "
            + "       var st = SortedTable.getSortedTable(v1); \n "
            + "       if (!st) return 0; \n "
            + "       v1 = st.elementIndex(v1); \n "
            + "       v2 = st.elementIndex(v2); \n "
            + "       if (v1==null || v2==null) return 0; \n "
            + "       else if (v1<v2) return 1 \n "
            + "       else if (v2<v1) return -1; \n "
            + "       return 0; \n "
            + "   }, \n "
            + "   move:function(d,elm) { \n "
            + "       if (elm) this.moverow(d,elm); \n "
            + "       else { \n "
            + "           var m = true; \n "
            + "           for (var i=0;i<this.selectedElements.length;i++) { \n "
            + "               if (!this.canMove(d,this.selectedElements[i])) m = false; \n "
            + "           } \n "
            + "           if (m) { \n "
            + "               var moving = this.selectedElements.slice(0,this.selectedElements.length); \n "
            + "               moving.sort(this.compareindex); \n "
            + "               if (d>0) moving.reverse(); \n "
            + "               for (var i=0;i<moving.length;i++) { \n "
            + "                   this.moverow(d,moving[i]); \n "
            + "               } \n "
            + "           } \n "
            + "       } \n "
            + "       if (typeof(this.onmove)=='function') this.onmove(d,elm); \n "
            + "   }, \n "
            + "   moverow:function(d,elm) { \n "
            + "       this.cleansort(); \n "
            + "       var parent = elm.parentNode; \n "
            + "       var sibling = this.canMove(d,elm); \n "
            + "       if (!sibling) return false; \n "
            + "       if (d>0) { \n "
            + "           parent.removeChild(elm); \n "
            + "           parent.insertBefore(elm,sibling); \n "
            + "       } else { \n "
            + "           parent.removeChild(elm); \n "
            + "           if (sibling.nextSibling) parent.insertBefore(elm,sibling.nextSibling); \n "
            + "           else parent.appendChild(elm); \n " + "       } \n "
            + "   }, \n " + "   canMove:function(d,elm) { \n "
            + "       if (d>0) return elm.previousSibling; \n "
            + "       else return elm.nextSibling; \n " + "   } \n " + "} \n "
            + "</script> \n ";

    String eventJs =
        "<script type=\"text/javascript\"> \n "
            + " \n "
            + "function addEvent(obj,type,fn) { \n "
            + "   if (obj.addEventListener) obj.addEventListener(type,fn,false); \n "
            + "   else if (obj.attachEvent)   { \n "
            + "       obj[\"e\"+type+fn] = fn; \n "
            + "       obj[type+fn] = function() {obj[\"e\"+type+fn](window.event);} \n "
            + "       obj.attachEvent(\"on\"+type, obj[type+fn]); \n "
            + "   } \n "
            + "} \n "
            + " \n "
            + "function removeEvent(obj,type,fn) { \n "
            + "   if (obj.removeEventListener) obj.removeEventListener(type,fn,false); \n "
            + "   else if (obj.detachEvent) { \n "
            + "       obj.detachEvent(\"on\"+type, obj[type+fn]); \n "
            + "       obj[type+fn] = null; \n " + "       obj[\"e\"+type+fn] = null; \n "
            + "   } \n " + "} \n " + " \n " + "</script> \n ";
    // eventJs = "";

    String beginStr =
        "<html> \n <head> \n "
            + "<style type=\"text/css\"> \n"
            + "TABLE{ \n background-color:#E6E6FA; \n} \n"
            + "body {background-color:#CCFFFF;} \n"
            + "h1 { \n padding:10px 10px 0px 10px; \n margin:0px; \n color:#EAEAEA; \n "
            + "h1.title { \n padding:10px 10px 0px 10px; \n margin:0px; \n color:rgb(255,255,255); \n } \n"
            + "font-family:Verdana, Tahoma; \n font-size : 20px; \n font-weight:bold; \n text-align:center; \n font-style: oblique; \n } \n"
            + "TABLE.title{ \n background-color:rgb(0,0,102); \n align:center; \n width:85%; \n} \n"
            + ".odd td {background-color:#E8ECF1;} \n"
            + ".even td {background-color:#DDE5EB;} \n"
            + ".hover td {background-color:#A5B3C9;} \n"
            + ".sortedminus {background-color:#ecc;} \n"
            + ".sortedplus {background-color:#cec;} \n"
            + ".selrow td {background-color:#879AB7;} \n" + "</style> \n" + eventJs
            + "\n" + sortedTableJs + "\n" + jsStr + "\n" + "<title> " + pageTitle
            + " </title>" + "</head> \n <body>\n";
    // +
    // "<table class=\"title\" cellpadding=\"2\" cellspacing=\"2\" border=\"0\" align=\"center\">"
    // + "<tbody>" + "<tr> <th class=\"title\"> <br>"
    // + "<h1 class=\"title\">PEOPLE SEARCH TEST RESULT ANALYSIS </h1> "
    // + "<br> </th> </tbody> </table> <br> <br> \n";
    return beginStr;
  }

  public static String getNonSortingHtmlBegin(String pageTitle)
  {
    String htmlBeginPart =
        "<html>\n"
            + "<head>\n"
            + "<META http-equiv=\"Content-Type\" content=\"text/html; charset=US-ASCII\">\n"
            + "<title> "
            + pageTitle
            + " </title>\n"
            + "<style type=\"text/css\">\n"
            + "body {\n"
            + "font:normal 68% verdana,arial,helvetica;\n"
            + "color:#000000;\n"
            + "}\n"
            + "table tr td, table tr th {\n"
            + "font-size: 68%;\n"
            + "}\n"
            + "table.details tr th{\n"
            + "font-weight: bold;\n"
            + "text-align:left;\n"
            + "background:#a6caf0;\n"
            + "white-space: nowrap;\n"
            + "}\n"
            + "table.details tr td{\n"
            + "background:#eeeee0;\n"
            + "white-space: nowrap;\n"
            + "}\n"
            + "h1 {\n"
            + "margin: 0px 0px 5px; font: 165% verdana,arial,helvetica\n"
            + "}\n"
            + "h2 {\n"
            + "margin-top: 1em; margin-bottom: 0.5em; font: bold 125% verdana,arial,helvetica\n"
            + "}\n"
            + "h3 {\n"
            + "margin-bottom: 0.5em; font: bold 115% verdana,arial,helvetica\n"
            + "}\n"
            + "h4 {\n"
            + "font:normal 115% verdana,arial,helvetica\n"
            + " }\n"
            + ".Failure {\n"
            + "font-weight:bold; color:red;\n"
            + "}\n"
            + "img\n"
            + "{\n"
            + "border-width: 0px;\n"
            + "}\n"
            + ".expand_link\n"
            + "{\n"
            + "position=absolute;\n"
            + "right: 0px;\n"
            + "width: 27px;\n"
            + "top: 1px;\n"
            + "height: 27px;\n"
            + "}\n"
            +

            ".page_details\n"
            + "{\n"
            + "display: none;\n"
            + "}\n"
            + ".page_details_expanded\n" + "{\n" + "display: block;\n" + "}\n" +

            "</style>\n </head> \n <body> \n ";
    return htmlBeginPart;
  }

  public static String getSimpleComparisonHeadingHtml(String[] expectedHeaders)
  {
    String result = "<tr valign=\"top\">\n";
    for (int i = 0; i < expectedHeaders.length; i++)
    {
      if (i == 0)
      {
        result =
            result + "<th title=\"" + expectedHeaders[i] + "\">" + expectedHeaders[i]
                + "</th> ";
      }
      else
      {
        result =
            result + "<th colspan=\"2\" title=\"" + "DIFFERENCE IN " + expectedHeaders[i]
                + "\">" + "DIFFERENCE IN " + expectedHeaders[i] + "</th> ";
      }
    }
    result = result + "\n</tr>\n";
    return result;
  }

  public static String getSimpleNormalHeadingHtml(String key, String[] expectedHeaders)
  {
    String result = "<tr valign=\"top\">\n";
    result = result + "<th title=\"" + key + "\">" + key + "</th> ";
    for (int i = 0; i < expectedHeaders.length; i++)
    {
      result =
          result + "<th title=\"" + expectedHeaders[i] + "\">" + expectedHeaders[i]
              + "</th> ";
    }
    result = result + "\n</tr>\n";
    return result;
  }

  public static String getSimpleNormalRowHtmlString(String key,
                                                    String[] expectedHeaders,
                                                    String[] values,
                                                    String versionOne)
  {
    String verString = "";
    if ((versionOne != null) && (!"".equals(versionOne.trim())))
    {
      verString = " in version " + versionOne;
    }
    String row = "<tr valign=\"top\">\n";
    row = row + "<td  rowspan=\"1\" valign=\"top\"> <b>" + key + "</b> </td>\n";
    for (int i = 0; i < expectedHeaders.length; i++)
    {
      row =
          row + "<td title=\"Value of " + expectedHeaders[i] + verString + "\">"
              + values[i] + "  </td>\n";
    }
    row = row + "</tr>\n";
    return row;
  }

  public static String getSimpleNormalRowHtmlString(String key,
                                                    GenericStatisticsUtil su,
                                                    String versionOne)
  {
    String[] expectedHeaders = GenericStatisticsUtil.getHeaders();
    String[] values =
        new String[] { su.getCount() + "", su.getMin() + "", su.getMax() + "",
            PerformanceManager.truncateNumber(su.getMean(), 3) + "", su.getMedian() + "",
            PerformanceManager.truncateNumber(su.getStandardDeviation(), 3) + "",
            su.getNthPercentile(90) + "" };
    String verString = "";
    if ((versionOne != null) && (!"".equals(versionOne.trim())))
    {
      verString = " in version " + versionOne;
    }
    String row = "<tr valign=\"top\">\n";
    row = row + "<td  rowspan=\"1\" valign=\"top\"> <b>" + key + "</b> </td>\n";
    for (int i = 0; i < expectedHeaders.length; i++)
    {
      row =
          row + "<td title=\"Value of " + expectedHeaders[i] + verString + "\">"
              + values[i] + "  </td>\n";
    }
    row = row + "</tr>\n";
    return row;
  }

  public static String getSimpleTableHtmlStringForGraphs(String[] strs,
                                                         String alignment,
                                                         int numOfImagesInRow)
  {
    int number = 2;
    if (numOfImagesInRow > 0)
    {
      number = numOfImagesInRow;
    }
    String align = "center";
    if ((alignment != null) && (!"".equalsIgnoreCase(alignment)))
    {
      align = alignment;
    }
    boolean isClosed = true;
    int numToBeFilled = (strs.length) % number;
    if (strs.length <= numOfImagesInRow)
    {
      numToBeFilled = 0;
    }
    String table =
        "<table class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"95%\">\n";
    if (strs != null)
    {
      for (int i = 0; i < strs.length; i++)
      {
        if (i == 0)
        {
          table = table + "<tr valign=\"top\">\n";
          table =
              table + "<td> <" + align + "> <img src=\"" + strs[i] + "\"> </" + align
                  + "> </td>\n";
          isClosed = false;
        }
        else
        {
          if ((i + 1) % number == 0)
          {
            table =
                table + "<td> <" + align + "> <img src=\"" + strs[i] + "\"> </" + align
                    + "> </td>\n";
            table = table + "</tr>\n";
            isClosed = true;
          }
          else
          {
            table =
                table + "<td> <" + align + "> <img src=\"" + strs[i] + "\"> </" + align
                    + "> </td>\n";
            isClosed = false;
          }
        }
      }
      if (!isClosed)
      {
        for (int i = 0; i < numToBeFilled; i++)
        {
          table = table + "<td> </td>\n";
        }
        table = table + "</tr>\n";
      }
    }
    table = table + "</table> \n";
    return table;

  }

  public static String getSimpleTableHtmlString(String key,
                                                String[] scenarios,
                                                GenericStatisticsUtil[] su,
                                                String versionOne)
  {
    String table =
        "<table class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"95%\">\n";
    table =
        table + getSimpleNormalHeadingHtml(key, GenericStatisticsUtil.getHeaders())
            + "\n";
    for (int i = 0; i < scenarios.length; i++)
    {
      table = table + getSimpleNormalRowHtmlString(scenarios[i], su[i], versionOne);
    }
    table = table + "</table> \n";
    return table;
  }

  public static String getSimpleTableHtmlString(String[] headings, String[] values)
  {
    String table =
        "<table class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"95%\">\n";
    String row = "";
    if (headings.length > 0)
    {
      row = "<tr valign=\"top\">\n";
      String key = headings[0];
      String[] tmpHeadings = new String[headings.length - 1];
      for (int i = 0; i < tmpHeadings.length; i++)
      {
        tmpHeadings[i] = headings[i + 1];
      }
      table = table + getSimpleNormalHeadingHtml(key, tmpHeadings) + "\n";
      row = row + "<td  rowspan=\"1\" valign=\"top\"> " + values[0] + " </td>\n";
      for (int i = 1; i < headings.length; i++)
      {
        row = row + "<td>" + values[i] + "  </td>\n";
      }
      row = row + "</tr>\n";
    }

    table = table + row + "</table> \n";
    return table;
  }

  public static String getSimpleTableHtmlString(String[] headings, String tableBody)
  {
    String table =
        "<table class=\"details\" border=\"0\" cellpadding=\"5\" cellspacing=\"2\" width=\"95%\">\n";
    String row = "";
    if (headings.length > 0)
    {
      row = "<tr valign=\"top\">\n";
      String key = headings[0];
      String[] tmpHeadings = new String[headings.length - 1];
      for (int i = 0; i < tmpHeadings.length; i++)
      {
        tmpHeadings[i] = headings[i + 1];
      }
      table = table + getSimpleNormalHeadingHtml(key, tmpHeadings) + "\n";
      row = tableBody + "\n";
    }

    table = table + row + "</table> \n";
    return table;
  }

  public static String getHtmlEnd()
  {
    String htmlEndPart = " </body> \n </html>\n";
    return htmlEndPart;
  }

  public void addTable(String heading, String table)
  {
    String result = " \n";
    if ((heading != null) && (!"".equals(heading.trim())))
    {
      result = result + "<h2> " + heading + "</h2> \n";
    }
    result = result + table + "<hr><br> \n";
    tableList.add(result);
  }

  public void createSimpleNormalHtmlFile(String outFile, String pageTitle) throws Exception
  {
    Writer writer = new FileWriter(new File(outFile), false);
    writer.write(getNonSortingHtmlBegin(pageTitle));
    writer.flush();

    Iterator itr = tableList.iterator();
    while (itr.hasNext())
    {
      writer.write((String) itr.next());
      writer.flush();
    }

    writer.write(getHtmlEnd());
    writer.flush();
    writer.close();
  }

}
