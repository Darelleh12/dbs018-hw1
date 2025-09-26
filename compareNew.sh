rm -f grade
base1="tc"
base="ans"
base2=".out"
sum=0
for testN in {1..10}
do

  inputAn=$base$testN$base2
  input=$base1$testN$base2

  declare -a anArray
  i=0
  while IFS=' ' read -ra line
  do
    if [ -z "$line" ]
    then
      continue
    fi
    #for word in "${line[@]}"
    #do
    #  echo $word
    #done
    
    if [ "$i" -eq 0 ]
    then
      i=$(($i+1))
      continue
    fi
    
    if [ "${line[0]}" = "DB" -a  "${line[1]}" = "referential" ] 
    then
      key=$(echo "${line[3]}")
      anArray[$i]=$key
      i=$(($i+1))
      continue
    fi

    if [ "${line[0]}" = "DB" -a  "${line[1]}" = "normalized:" ]
    then
      key=$(echo "${line[2]}")
      
      key=${key//$'\n'/} 
      anArray[$i]=$key
      i=$(($i+1))
      continue
    fi
 
    key=$(echo "${line[1]}")
    anArray[$i]=$key
    #echo ${line[1]}
    i=$(($i+1))

    key=$(echo "${line[2]}")
    anArray[$i]=$key
    i=$(($i+1))
  done < $inputAn

  #printf '%s\n' "${anArray[@]}"

  i=0
  right=0
  while IFS=' ' read -ra line || [ -n "$line" ]
  do
    if [ -z "$line" ]
    then
      continue
    fi
    #for word in "${line[@]}"
    #do
    #  echo $word
    #done

    if [[ "$i" -eq 0 ]]
    then
      i=$(($i+1))
      continue
    fi

    if [ "${line[0]}" = "DB" -a  "${line[1]}" = "referential" ]
    then
      
      if [ ${anArray[$i]} == ${line[3]} ]
      then
        right=$(($right+1))
      fi
      i=$(($i+1))
      continue
    fi

    if [ "${line[0]}" = "DB" -a  "${line[1]}" = "normalized:" ]
    then
      temp=${line[2]}
      temp=${temp//$'\n'/} 
      #if [[ ${anArray[$i]} == ${line[2]} ]]
      if [[ ${anArray[$i]} == $temp ]]
      then
        right=$(($right+1))
      fi
      i=$(($i+1))
      continue
    fi

    if [[ ${anArray[$i]} == ${line[1]} ]]
    then
        right=$(($right+1))
    fi

    i=$(($i+1))
    if [[ ${anArray[$i]} == ${line[2]} ]]
    then
        right=$(($right+1))
    fi
    i=$(($i+1))
  done < $input
  
  #echo $right
  length=${#anArray[@]}
  #echo $length
  
  wrong=$((length - right))  
  z=$((10*wrong/length))
  sum=$((sum + 10 - z))
  echo $sum
done
echo $sum > grade
