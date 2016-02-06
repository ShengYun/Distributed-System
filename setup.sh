if [ "$HOSTNAME" = csa2.bu.edu ]; then
	printf '%s\n' "On csa2, setting up GOROOT and bin"
	echo "Setting GOROOT"
	export GOROOT=/research/sesa/451/go
	echo "Setting Go Bin"
	export PATH=$PATH:$GOROOT/bin
else
	printf '%s\n' "On local machine"
fi

echo "setting up GOPATH"
export GOPATH=$PWD
