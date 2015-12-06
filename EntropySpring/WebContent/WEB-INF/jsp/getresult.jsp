<%@ page contentType="text/html; charset=UTF-8" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<head>
<title>Entropy result</title>
</head>
<body>
   <h2>${message}</h2>
   
 <c:forEach var="map" items="${myMap}">
    Password: ${map.key} Entropy: ${map.value}
    <br />
</c:forEach>

</body>
</html>