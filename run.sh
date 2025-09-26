printf "-------------Evalution start----------\n"

base=113
pre="cosc0"
pre2="tc"
pre3=".out"
pre4=".txt"
pre5="ans"
pre6=".sql"



for((i=0; i<1; i++))
do
	cd /mdata/cosc/y2020/fall/cs3380-2/  
	n=$(($base+$i)) 
	str=$pre$n
	printf "\n we are evaluting the account $str\n"
	
	cd $str
	rm -r hw2_grade
	mkdir hw2_grade
	
	cp hw2/checkdb.py hw2_grade/
         	
        
	cp hw2/ReadMe hw2_grade/
	cp hw2/readMe hw2_grade/
	cp hw2/readme hw2_grade/
	cp hw2/README hw2_grade/
	
        cp /mdata/cosc/y2020/fall/cs3380-2/cosc0217/hw2/compareNew.sh hw2_grade/
	cd hw2_grade
	##copy ans.out files
	for((j=1; j<=10; j++))
	do
		testAn=$pre5$j$pre3
	        testCase=$pre2$j$pre4
                testsql=$pre2$j$pre6
                testOut=$pre2$j$pre3                

		cp /mdata/cosc/y2020/fall/cs3380-2/cosc0217/hw2/$testAn .
                cp /mdata/cosc/y2020/fall/cs3380-2/cosc0217/hw2/$testCase .
                cp /mdata/cosc/y2020/fall/cs3380-2/cosc0217/hw2/$testsql .
           
                psql -d COSC3380 -f $testsql
                python3 checkdb.py database=$testCase > $testOut
                                                	
     
	done
 
        
        
	
        ./compareNew.sh
	rm -f compareNew.sh
        chmod 744 grade


done





