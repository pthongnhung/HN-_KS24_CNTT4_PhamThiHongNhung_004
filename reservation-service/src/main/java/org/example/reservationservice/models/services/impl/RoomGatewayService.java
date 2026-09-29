package org.example.reservationservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.reservationservice.clients.RoomClient;
import org.example.reservationservice.exceptions.RoomNotFoundException;
import org.example.reservationservice.exceptions.RoomServiceException;
import org.example.reservationservice.models.dto.responses.RoomResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RoomGatewayService {

    private final RoomClient roomClient;

    @CircuitBreaker(name = "roomService", fallbackMethod = "getRoomByIdFallback")
    public RoomResponse getRoomById(Long roomId) {
        try {
            RoomResponse room = roomClient.getRoomById(roomId);
            if (room == null || room.roomId() == null || room.pricePerNight() == null) {
                throw new RoomServiceException("Invalid response from room service for room id: " + roomId);
            }
            return room;
        } catch (FeignException.NotFound exception) {
            throw new RoomNotFoundException(roomId);
        } catch (FeignException exception) {
            throw new RoomServiceException("Room service is unavailable for room id: " + roomId, exception);
        }
    }

    public RoomResponse getRoomByIdFallback(Long roomId, Throwable throwable) {
        if (throwable instanceof RoomNotFoundException roomNotFoundException) {
            throw roomNotFoundException;
        }
        if (throwable instanceof RoomServiceException roomServiceException) {
            throw roomServiceException;
        }
        throw new RoomServiceException("Room service is unavailable for room id: " + roomId, throwable);
    }
}
