if [ -f inputs.json ]; then
  rm inputs.json
fi

if [ -f response.json ]; then
  rm response.json
fi

if [ -f workflow_response.json ]; then
  rm workflow_response.json
else
  echo "No workflow response file found"
  exit 1
fi