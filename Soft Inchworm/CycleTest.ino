#define PUMP 2
#define VALVE 3

void setup() {
  // put your setup code here, to run once:
  pinMode(PUMP, OUTPUT);
  pinMode(VALVE, OUTPUT);
}

void loop() {
  // put your main code here, to run repeatedly:
  digitalWrite(PUMP, HIGH);
  digitalWrite(VALVE, LOW);
  delay(400);
  digitalWrite(PUMP, LOW);
  digitalWrite(VALVE, HIGH);
  delay(4000);
}
