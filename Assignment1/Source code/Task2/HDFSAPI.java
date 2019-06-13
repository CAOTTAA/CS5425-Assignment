package task2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.text.SimpleDateFormat;

public class HDFSAPI {
	Configuration conf = new Configuration();
	private FileSystem hdfs;

	public HDFSAPI(Path hdfsPath) throws IOException{
		hdfs = hdfsPath.getFileSystem(conf);
	}
	/**
	 * create directory
	 * @param path
	 * @throws IOException
	 */
	public void mkDir(Path path) throws IOException{		
		hdfs.mkdirs(path);
	}
	/**
	 * upload file
	 * @param src
	 * @param dst
	 * @throws IOException
	 */
	public void copyLocalToHdfs(Path src,Path dst) throws IOException{
		hdfs.copyFromLocalFile(src, dst);
	}
	/**
	 * delete file
	 * @param path
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void delFile(Path path) throws IOException{
		hdfs.delete(path);
	}
	/**
	 * read file
	 * @param path
	 * @throws IOException
	 */
	public void readFile(Path path) throws IOException{
		//get file info
		FileStatus filestatus = hdfs.getFileStatus(path);
		
		FSDataInputStream in = hdfs.open(path);
		IOUtils.copyBytes(in,System.out,(int)filestatus.getLen(),false);
		System.out.println();
	}
	/**
	 * get the time the file changed 
	 * @param path
	 * @throws IOException
	 */
	public void getModifyTime(Path path) throws IOException{
		FileStatus files[] = hdfs.listStatus(path);
		for(FileStatus file: files){
			//time = file.getModificationTime().
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String date = sdf.format(file.getModificationTime());
			System.out.println(file.getPath()+"\t"+date);
		}
	}
	/**
	 * create file and write in hdfs
	 * @param path
	 * @param content
	 * @throws IOException
	 */
	public void writeFile(Path path,String content) throws IOException{
		FSDataOutputStream os = hdfs.create(path);
		//utf-8
		os.write(content.getBytes("UTF-8"));
		os.close();
	}
	/**
	 * list the file
	 * @param path
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public void listFiles(Path path) throws IOException{
		hdfs = path.getFileSystem(conf);
		FileStatus files[] = hdfs.listStatus(path);
		int listlength=files.length;  
        for (int i=0 ;i<listlength ;i++){  
            if (files[i].isDirectory() == false) {  
                System.out.println("filename:"  
                		+ files[i].getPath()  + "\tsize:"  
                        + files[i].getLen());  
            } else {  
                Path newpath = new Path(files[i].getPath().toString());  
                listFiles(newpath);  
            }  
        }  
	}
}