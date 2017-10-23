# precondition: log_check contain all vm*.log in current machine, vmi.log in ith machine's current executable path
# input: query string
if [ "$#" -ne 1 ]; then
    echo "Must have exactly 1 argument after executable indicates query"
fi
for x in {1..10}
do
	> log_query${x}.txt
done
> temp2
./../client/log_grep $1 vm*.log > tempCount

for x in {1..10}
do
	echo checking error for machine $x  ....
	#log_check: contain original files; log_query: contain result returned by query
	grep $1 ../log_check/vm${x}.log > expected_result_grep_${x}
	if  ! cmp -s log_query${x}.txt expected_result_grep_${x}; then
		echo file differs: log_query${x}.txt, expected_result_grep_${x}
	fi
		
	grep --count $1 ../log_check/vm${x}.log > temp1 
	# tempCount: output for command (vm1: ....), temp1: string of int represent count of the file, temp2: concat of temp1
	python check_diff_count.py tempCount temp1 $x
	cat temp1 >> temp2
done

python check_diff_count.py tempCount temp2 11
echo DONE
