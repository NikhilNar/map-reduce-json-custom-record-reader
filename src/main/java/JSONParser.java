class JSONParse{
    public static void main(String[] args){
        String str="{\n" +
                "  \"name\":\"nikhil\",\n" +
                "  \"address\":\"Brooklyn\",\n" +
                "  \"college\":{\n" +
                "    \"name\":\"NYU\",\n" +
                "    \"address\":\"Brooklyn\"\n" +
                "  }\n" +
                "}";
        System.out.println(str);
    }
}