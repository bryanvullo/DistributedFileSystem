import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index {
    
    public Map <Integer, List<String>> port2files;
    public Map <String, List<Integer>> file2ports;
    public Map <String, Status> fileStatus;
    public Map <String, Integer> fileSizes;
    
    public Index() {
        this.port2files = new HashMap<>();
        this.file2ports = new HashMap<>();
        this.fileStatus = new HashMap<>();
        this.fileSizes = new HashMap<>();
    }
    
    
    public void removeFileStoreFailed(String fileName) {
        fileStatus.remove(fileName);
        fileSizes.remove(fileName);
        file2ports.remove(fileName);
    }
    
    public void removeFileRemoveComplete(String fileName) {
        fileStatus.remove(fileName);
        fileSizes.remove(fileName);
        file2ports.remove(fileName);
    }
}
