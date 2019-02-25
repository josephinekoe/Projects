/*
File System:
PrimeNumberListFolder/PrimeNumberList_ //contains name of last file in numbered list
PrimeNumberListFolder/PrimeNumberList_0 //contains number 2
PrimeNumberListFolder/PrimeNumberList_1 //contains first 10,000,000 prime numbers (excluding 2)
PrimeNumberListFolder/PrimeNumberList_2 //contains next 10 M primes
PrimeNumberListFolder/PrimeNumberList_3 //contains next 10 M primes
...
*/
package primeGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CopyOfPrimeGenerator{
    
    private static String firstFileName = System.getProperty("user.dir") + "/PrimeNumberListFolder/PrimeNumberList_";//contains name of last file in numbered list
    private static File file = null;
    private static File lastFile = null;
    private static BigInteger lastFileNum = null;
    private static Scanner scanner = null;
    private static String string = null;
    private static int numCounter = 0;
    private final long fileMax = 135000000*8;//number of bytes in each file
    private ArrayList<String> strings = new ArrayList<String>();
    private static ArrayList<BigInteger> array1 = null;
    private FileWriter fw = null;
    private PrintWriter pw = null;
    private Integer serialJobNum = 5000;
    private Integer processorNum = Runtime.getRuntime().availableProcessors();
    private int parallelJobsNum = serialJobNum*processorNum;
    private BigInteger lastPrime = null;
    private BigInteger[][] candidatePrime = new BigInteger[2][parallelJobsNum];
    private boolean[][] compositeFlag = new boolean[2][parallelJobsNum];
    private boolean[] boolean1 = {true, false}; // composite and don't check further
    private boolean[] boolean2 = {false, false}; // prime and don't check further (confirmed prime)
    private boolean[] boolean3 = {false, true}; // not composite yet need to check further
    private ExecutorService es = null;
    private boolean firstArray = true;
    private byte a;
    private int numOfBits;
    private long begTest;
    private long endTest;
    private double difference;
    
    public static void main(String[] args){
          new CopyOfPrimeGenerator().method();
    }

    public void method(){
        begTest = new java.util.Date().getTime();
        lastPrime = getLastPrime(); 
        numOfBits = lastPrime.bitCount();
        array1 = new PrimeCheck().readArray(new BigInteger("1"));//read first file
        System.out.println("readArray");
        es = Executors.newFixedThreadPool(processorNum);
        while(true){    
            if(firstArray)
                a = 0;
            else
                a = 1;
            for(int i = 0; i < processorNum; i++){
                es.execute(new PrimeCheck(i));
            }
            es.shutdown();
            while (es.isTerminated() == false);
            es = Executors.newFixedThreadPool(processorNum);
            if(firstArray){
                es.execute(new Writer(candidatePrime[0], compositeFlag[0]));
                lastPrime = candidatePrime[0][candidatePrime[0].length-1];
            }
            else{
                es.execute(new Writer(candidatePrime[1], compositeFlag[1]));
                lastPrime = candidatePrime[1][candidatePrime[1].length-1];
            }
            firstArray = !firstArray;
            endTest = new java.util.Date().getTime();
            difference = (endTest-begTest)*0.001;
            System.out.println(difference);
            begTest = endTest;
        }
    }   
    
    public BigInteger getLastPrime(){
        file = new File(firstFileName);
        try {
            scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        string = scanner.nextLine();//string has name of last file
        lastFile = new File(string);
        scanner.close();
        lastFileNum = new BigInteger(string.substring(firstFileName.length()));//number of last file
        try {
            scanner = new Scanner(lastFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        do{
            string = scanner.nextLine();
            numCounter++;//count number of lines(numbers) in file
        }while(scanner.hasNext());
        scanner.close();//string has value of largest prime number + ","
        string = string.substring(0, string.length()-1);
        return new BigInteger(string);
    }
    
    
    
    private class PrimeCheck implements Runnable{
        
        private Scanner s = null;
        private BigInteger two = new BigInteger("2");
        private int index = 0;
        private boolean[] results = null;
        private ArrayList<BigInteger> array2 = new ArrayList<BigInteger>();
        private int candidateIndex = 0;
        private BigInteger testNum = null;
        private BigInteger fileCounter = new BigInteger("0");
        private int iXserialJobsX2;
        private int iP1X2;
        private Integer sum;
        
        public PrimeCheck(){}

        private PrimeCheck(int _index){
            index = _index;
            iXserialJobsX2 = index*serialJobNum*2;
        }
        
        public ArrayList<BigInteger> readArray(BigInteger counter){
            file = new File(firstFileName+counter.toString());
            try {
                s = new Scanner(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            do{
                strings.add(s.nextLine());//assign Strings into array
            }while(s.hasNext());
            s.close();      
            ArrayList<BigInteger> array = new ArrayList<BigInteger>();
            for(int i = 0; i < strings.size(); i++){
                array.add(new BigInteger(strings.get(i).substring(0, strings.get(i).length()-1)));//assign parsed BigIntegers into array
            }
            return array;
        }

        public void run(){
            for(int i = 0; i<serialJobNum; i++) {
                candidateIndex = index*serialJobNum + i;
                iP1X2 = (index+1)*2;
                sum = iXserialJobsX2 + iP1X2;
                candidatePrime[a][candidateIndex] = lastPrime.add(new BigInteger(sum.toString()));
                compositeFlag[a][candidateIndex] = false;
                testNum = candidatePrime[a][candidateIndex];
                if(testNum.equals((testNum.subtract(new BigInteger("1"))).nextProbablePrime())){//miller rabin primality test (does not skip any primes)
                    results = arrayCompare(array1, testNum);
                    if(results.equals(boolean3)){//if prime so far, check further 
                        if(!(fileCounter.equals(two))){
                            fileCounter = new BigInteger("2");
                            array2 = readArray(fileCounter);
                        }
                        results = arrayCompare(array2, testNum);
                        while(results.equals(boolean3)){//if prime so far, check further
                            fileCounter = fileCounter.add(new BigInteger("1"));
                            array2 = readArray(fileCounter);
                            results = arrayCompare(array2, testNum);
                        }
                    }
                    compositeFlag[a][candidateIndex] = results[0];
                }
                else
                    compositeFlag[a][candidateIndex] = true;
            }
        }
        
        public boolean[] arrayCompare(ArrayList<BigInteger> array, BigInteger _possiblePrime){
            for(int i = 0; i < array.size(); i++){  
                if(_possiblePrime.compareTo(array.get(i).pow(2))>=0){//if possiblePrime is greater than divisor^2
                    if(_possiblePrime.remainder(array.get(i)) == BigInteger.ZERO){//if possiblePrime is divisible
                        return boolean1;//composite, do not check further
                    }
                }
                else return boolean2;//prime, do not check further
            }       
            return boolean3;//prime so far, check further 
        }
    }
    
    private class Writer implements Runnable{
        
        private BigInteger[] candidates = null;
        private boolean[] composite = null;
        
        public Writer(BigInteger[] _candidates, boolean[] _composite){
            candidates = _candidates;
            composite = _composite;
        }
        
        public void run() {
            try {
                fw = new FileWriter(lastFile, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            pw = new PrintWriter(fw);
            for(int i = 0; i < composite.length; i++){
                if(composite[i]==false){
                    if(numCounter>=fileMax/numOfBits)//if current file is filled
                        writeNewFile(candidates[i]);
                    else
                        write(candidates[i]);
                }
            }   
        }
        
        public void write(BigInteger number){
            numCounter++;
            pw.append("\n"+number+",");
            pw.flush();
        }       
        
        public void writeNewFile(BigInteger number){
            pw.flush();
            pw.close();
            numCounter = 1;//start new line count
            lastFileNum = lastFileNum.add(new BigInteger("1"));
            lastFile = new File(firstFileName+lastFileNum);//start new file
            try {
                fw = new FileWriter(firstFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            pw = new PrintWriter(fw);
            pw.print(lastFile.getPath());//print name of largest file into first file
            pw.flush();
            pw.close();
            try {
                fw = new FileWriter(lastFile, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            pw = new PrintWriter(fw);
            pw.append(number+",");//write to new file
            pw.flush();
        }       
    }
}