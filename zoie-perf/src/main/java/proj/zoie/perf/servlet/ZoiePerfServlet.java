package proj.zoie.perf.servlet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ZoiePerfServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private File _dataDir = null;
  private Map<String, RandomAccessFile> _readerMap = new HashMap<String, RandomAccessFile>();

  private Map<String, File> _fileMap = new HashMap<String, File>();

  public ZoiePerfServlet(File dataDir) {
    _dataDir = dataDir;
    if (!_dataDir.exists()) {
      _dataDir.mkdirs();
    }

    _fileMap.put("searchTimer", new File(_dataDir, "searchTimer.csv"));
    _fileMap.put("consumeRateCount", new File(_dataDir, "consumeRateCount.csv"));
    _fileMap.put("consumeRateMB", new File(_dataDir, "consumeRateMB.csv"));
    _fileMap.put("indexLatency", new File(_dataDir, "indexLatency.csv"));
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse res) throws ServletException,
      IOException {
    String type = req.getParameter("type");
    File file = _fileMap.get(type);
    if (file == null) {
      throw new ServletException("invalid: " + type);
    }
    RandomAccessFile reader = _readerMap.get(type);
    if (reader == null) {
      reader = new RandomAccessFile(file, "r");
      _readerMap.put(type, reader);
    }

    long offset = 0;
    try {
      offset = Long.parseLong(req.getParameter("offset"));
    } catch (Exception e) {
      offset = 0L;
    }

    if (offset < reader.length()) {
      reader.seek(offset);
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          res.getWriter().print("");
          return;
        }
        if (line.trim().startsWith("#")) continue;
        long newOffset = reader.getFilePointer();
        res.getWriter().print(newOffset + ":" + line);
        return;
      }
    }
  }

  @Override
  public void destroy() {
    for (RandomAccessFile reader : _readerMap.values()) {
      try {
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
