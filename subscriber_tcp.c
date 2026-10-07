/*
 * subscriber_tcp.c
 * 
 * suscriptor del sistema, usando TCP. Se conecta al brocker, se suscribe a un partido,
 * y luego se queda esperando en los mensajes que el broker le
 * reenvíe, imprimiéndolos en pantalla en tiempo real.
 *
 * Protocolo de aplicación:
 *   Envía  -> SUB|<tema>|\n                 (Inicio de conexion)
 *   Recibe -> MSG|<tema>|<contenido>\n       (cada vez que hay una publicación)
 *
 * Compilar:
 *   gcc subscriber_tcp.c -o subscriber_tcp
 *
 * Ejecutar:
 *   ./subscriber_tcp <IP_broker> <puerto_broker> <tema>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>       // close()
#include <arpa/inet.h>      // inet_pton(), htons()
#include <sys/socket.h>     // socket(), connect(), send(), recv()
#include <netinet/in.h>     // struct sockaddr_in

#define TAM_BUFFER 1024

int main(int argc, char *argv[]) {

    if (argc != 4) {
        fprintf(stderr, "Uso: %s <IP_broker> <puerto_broker> <tema>\n", argv[0]);
        fprintf(stderr, "Ejemplo: %s 127.0.0.1 5000 EquipoA_vs_EquipoB\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *ip_broker = argv[1]; //coge la primera parte de la linea de comandos que corresponde a la ip del broker
    int puerto_broker = atoi(argv[2]); //coge la segunda parte de la linea de comandos que corresponde al puerto del broker
    const char *tema = argv[3]; // coge la tercera parte de la linea de comandos que corresponde al tema al que se quiere suscribir el suscriptor

    
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0); // crea el descriptor del socket.
    //   AF_INET     -> IPv4
    //   SOCK_STREAM -> TCP (orientado a conexión)
    if (socket_fd < 0) {
        perror("Error al crear el socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in direccion_broker;
    direccion_broker.sin_family = AF_INET;
    direccion_broker.sin_port = htons(puerto_broker); //  host-to-network short

    
    if (inet_pton(AF_INET, ip_broker, &direccion_broker.sin_addr) <= 0) { // convierte la IP en formato texto a su representación
        fprintf(stderr, "Direccion IP invalida: %s\n", ip_broker);
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    if (connect(socket_fd, (struct sockaddr *)&direccion_broker, sizeof(direccion_broker)) < 0) {  // establece la conexión TCP con el broker (dispara el three-way handshake).

        perror("Error al conectar con el broker");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    printf("[Subscriber] Conectado al broker %s:%d\n", ip_broker, puerto_broker);

    // Construir y enviar el mensaje de suscripción: SUB|tema|
    char mensaje_sub[TAM_BUFFER];
    snprintf(mensaje_sub, sizeof(mensaje_sub), "SUB|%s|\n", tema);

    if (send(socket_fd, mensaje_sub, strlen(mensaje_sub), 0) < 0) { // envía la solicitud de suscripción al broker. El broker registra este socket como interesado en el tema indicado.
        perror("Error al enviar la suscripcion");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    printf("[Subscriber] Suscrito al tema: %s\n", tema);
    printf("[Subscriber] Esperando mensajes... (Ctrl+C para salir)\n\n");

    char buffer[TAM_BUFFER];

    // desde aqui el subscriber no envía más mensajes propios, solo se queda esperando lo que el broker le reenvíe
    while (1) {
        memset(buffer, 0, TAM_BUFFER);

        // bloquea el proceso hasta que llegue un mensaje nuevo del broker, hasta que la conexión se cierre, o haya un error.
        int bytes_leidos = recv(socket_fd, buffer, TAM_BUFFER - 1, 0);

        if (bytes_leidos <= 0) {
            printf("\n[Subscriber] El broker cerro la conexion.\n");
            break;
        }

        // Quitar el salto de línea final si vino incluido.
        buffer[strcspn(buffer, "\n")] = '\0';

        //  Parseo del mensaje recibido: MSG|tema|contenido 
        char tipo[8] = {0}; //tipo de mensaje
        char tema_recibido[64] = {0}; //tema asociado al mensaje (partido)
        char contenido[TAM_BUFFER] = {0};  //contenido del mensaje (evento durante el partido)

        char copia[TAM_BUFFER];
        strncpy(copia, buffer, TAM_BUFFER - 1);
        copia[TAM_BUFFER - 1] = '\0';

        char *token = strtok(copia, "|");
        if (token != NULL) strncpy(tipo, token, sizeof(tipo) - 1);

        token = strtok(NULL, "|");
        if (token != NULL) strncpy(tema_recibido, token, sizeof(tema_recibido) - 1);

        token = strtok(NULL, "|");
        if (token != NULL) strncpy(contenido, token, sizeof(contenido) - 1);

        if (strcmp(tipo, "MSG") == 0) {
            printf(">> [%s] %s\n", tema_recibido, contenido);
        } else {
            // Por si llega algo con otro formato por error
            printf(">> (mensaje sin formato esperado): %s\n", buffer);
        }
    }

    // close(): cierra la conexión TCP de forma ordenada.
    close(socket_fd);

    return 0;
}